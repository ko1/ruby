# Crash via real raise: service raises on :boom (stress disabled just before);
# main rescues Ractor::RemoteError from #value, respawns from replicated log.
# Axes: 2 generations, 80 ops, copy, GC.start scattered in main (no stress in svc).
Warning[:experimental] = false
Thread.report_on_exception = false # deliberate crash below; keep output clean
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
spawn = lambda do |events|
  Ractor.new(done, events) do |done, events|
    state = Hash.new(0)
    log = []
    events.each { |k, d| state[k] += d; log << [k, d] }
    r = loop do
      msg = Ractor.receive
      break :stopped if msg == :stop
      op, k, d, rp = msg
      case op
      when :apply
        state[k] += d
        log << [k, d]
        rp << log.size
      when :boom
        GC.stress = false
        rp << log
        raise "service crashed deliberately"
      end
    end
    done << :done if r == :stopped
    [state.to_a.to_h, log.size]
  end
end
rp = Ractor::Port.new
model = Hash.new(0)
rng = Random.new(74)
svc = spawn.call([])
40.times do |i|
  k = "k#{rng.rand(9)}"
  d = rng.rand(1..7)
  model[k] += d
  svc.send([:apply, k, d, rp])
  raise unless rp.receive == i + 1
  GC.start if i % 16 == 15
end
svc.send([:boom, nil, nil, rp])
log = rp.receive
begin
  svc.value
  raise "expected RemoteError"
rescue Ractor::RemoteError => e
  raise "cause" unless e.cause.message == "service crashed deliberately"
end
raise unless log.size == 40
svc = spawn.call(log)
40.times do |i|
  k = "k#{rng.rand(9)}"
  d = rng.rand(1..7)
  model[k] += d
  svc.send([:apply, k, d, rp])
  raise "gen2" unless rp.receive == 41 + i
  GC.start if i % 16 == 15
end
svc.send(:stop)
done.receive
st, n = svc.value
raise "final" unless st == model && n == 80
puts "OK d74_respawn_raise_rescue"
