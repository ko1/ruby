# Append-only log alternating app-level compaction and GC.compact inside the
# service; state fold stays correct across 4 rounds. Axes: 4x30 appends, copy,
# stress in service.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  log = []
  rounds = 0
  loop do
    msg = Ractor.receive
    break if msg == :stop
    op, k, v, rp = msg
    case op
    when :append then log << [k, v]; rp << log.size
    when :round
      seen = {}
      log.reverse_each { |lk, lv| seen[lk] = lv unless seen.key?(lk) }
      log = seen.to_a.reverse
      GC.compact
      GC.start
      rounds += 1
      rp << [rounds, log.size]
    when :state
      rp << log.each_with_object({}) { |(lk, lv), h| h[lk] = lv }
    end
  end
  GC.stress = false
  done << :done
  rounds
end
rp = Ractor::Port.new
rng = Random.new(65)
model = {}
4.times do |round|
  30.times do |i|
    k = "k#{rng.rand(7)}"
    v = "r#{round}v#{i}"
    model[k] = v
    svc.send([:append, k, v, rp])
    rp.receive
  end
  svc.send([:round, nil, nil, rp])
  r, len = rp.receive
  raise "round#{round}" unless r == round + 1 && len == model.size
  svc.send([:state, nil, nil, rp])
  raise "state#{round}" unless rp.receive == model
end
svc.send(:stop)
done.receive
raise unless svc.value == 4
puts "OK d65_alog_gc_compact"
