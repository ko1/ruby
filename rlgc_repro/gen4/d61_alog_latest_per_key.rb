# Append-only log service; :compact retains only the latest record per key while
# preserving order; reads before/after agree. Axes: 130 appends / 10 keys, copy,
# stress in service.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  log = []
  loop do
    msg = Ractor.receive
    break if msg == :stop
    op, k, v, rp = msg
    case op
    when :append then log << [k, v]; rp << log.size
    when :read
      ent = log.reverse_each.find { |lk, _| lk == k }
      rp << (ent && ent[1])
    when :compact
      last = {}
      log.each_with_index { |(lk, _), idx| last[lk] = idx }
      log = log.each_with_index.select { |(lk, _), idx| last[lk] == idx }.map(&:first)
      rp << log.size
    end
  end
  GC.stress = false
  done << :done
  log
end
rp = Ractor::Port.new
rng = Random.new(61)
model = {}
order = []
130.times do |i|
  k = "k#{rng.rand(10)}"
  v = "v#{i}"
  model[k] = v
  order.delete(k)
  order << k
  svc.send([:append, k, v, rp])
  raise unless rp.receive == i + 1
end
before = model.keys.map { |k| svc.send([:read, k, nil, rp]); rp.receive }
svc.send([:compact, nil, nil, rp])
raise "clen" unless rp.receive == model.size
after = model.keys.map { |k| svc.send([:read, k, nil, rp]); rp.receive }
raise "reads changed" unless before == after && after == model.values
svc.send(:stop)
done.receive
log = svc.value
raise "final log" unless log == order.map { |k| [k, model[k]] }
puts "OK d61_alog_latest_per_key"
