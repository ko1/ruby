# Event-sourced service crash mid-stream; respawned service rebuilds by replaying
# the event log returned at crash, then continues. Axes: 2 gens, 120 events, copy,
# stress in services.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
spawn = lambda do |events|
  Ractor.new(done, events, STRESS) do |done, events, stress|
    GC.stress = true if stress
    state = Hash.new(0)
    log = []
    events.each { |k, d| state[k] += d; log << [k, d] }
    loop do
      msg = Ractor.receive
      break if msg == :stop
      op, ev, rp = msg
      case op
      when :apply
        k, d = ev
        state[k] += d
        log << ev
        rp << state[k]
      when :crash
        GC.stress = false
        rp << log
        break
      end
    end
    GC.stress = false
    done << :done
    state
  end
end
rp = Ractor::Port.new
model = Hash.new(0)
rng = Random.new(25)
svc = spawn.call([])
60.times do
  k = "e#{rng.rand(10)}"
  d = rng.rand(1..6)
  model[k] += d
  svc.send([:apply, [k, d], rp])
  raise unless rp.receive == model[k]
end
svc.send([:crash, nil, rp])
log = rp.receive
done.receive
svc.value
raise "log" unless log.size == 60
svc = spawn.call(log)
60.times do
  k = "e#{rng.rand(10)}"
  d = rng.rand(1..6)
  model[k] += d
  svc.send([:apply, [k, d], rp])
  raise "gen2" unless rp.receive == model[k]
end
svc.send(:stop)
done.receive
raise "final" unless svc.value == model
puts "OK d25_es_crash_rebuild"
