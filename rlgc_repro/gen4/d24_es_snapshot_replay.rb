# Event sourcing with snapshots: service snapshots state every 25 events and
# keeps only the tail; final state must equal snapshot + tail replay.
# Axes: 1 service, 130 events, copy, stress in service.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  state = Hash.new(0)
  snap = {}
  tail = []
  n = 0
  loop do
    msg = Ractor.receive
    break if msg == :stop
    ev, rp = msg
    k, d = ev
    state[k] += d
    tail << ev
    n += 1
    if n % 25 == 0
      snap = state.dup
      tail = []
    end
    rp << n
  end
  GC.stress = false
  done << :done
  [state, snap, tail]
end
rp = Ractor::Port.new
rng = Random.new(24)
model = Hash.new(0)
130.times do |i|
  k = "a#{rng.rand(8)}"
  d = rng.rand(-3..7)
  model[k] += d
  svc.send([[k, d], rp])
  raise unless rp.receive == i + 1
end
svc.send(:stop)
done.receive
state, snap, tail = svc.value
raise "tail len" unless tail.size == 130 % 25
rebuilt = Hash.new(0)
snap.each { |k, v| rebuilt[k] = v }
tail.each { |k, d| rebuilt[k] += d }
raise "rebuild" unless rebuilt == state
raise "model" unless state == model
puts "OK d24_es_snapshot_replay"
