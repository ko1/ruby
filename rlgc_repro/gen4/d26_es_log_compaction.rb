# Event log with app-level compaction: :compact keeps only last set per key;
# fold(compacted) must equal live state; log shrinks. Axes: 140 sets over 12 keys,
# copy, stress in service, GC.compact after app compaction.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  state = {}
  log = []
  loop do
    msg = Ractor.receive
    break if msg == :stop
    op, k, v, rp = msg
    case op
    when :set
      state[k] = v
      log << [k, v]
      rp << log.size
    when :compact
      seen = {}
      log.reverse_each { |lk, lv| seen[lk] = lv unless seen.key?(lk) }
      log = seen.to_a.reverse.map { |lk, lv| [lk, lv] }
      GC.compact
      rp << log.size
    end
  end
  GC.stress = false
  done << :done
  [state, log]
end
rp = Ractor::Port.new
rng = Random.new(26)
model = {}
140.times do |i|
  k = "k#{rng.rand(12)}"
  v = "v#{i}"
  model[k] = v
  svc.send([:set, k, v, rp])
  rp.receive
end
svc.send([:compact, nil, nil, rp])
clen = rp.receive
raise "compacted len #{clen}" unless clen == model.size
svc.send(:stop)
done.receive
state, log = svc.value
raise "state" unless state == model
folded = log.each_with_object({}) { |(k, v), h| h[k] = v }
raise "fold(compacted)" unless folded == state
puts "OK d26_es_log_compaction"
