# KV service with put/delete/size; size invariant tracked against main-side model.
# Axes: 1 service, 100 mixed ops seeded rng, copy, stress both sides, GC.start scattered.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  db = {}
  loop do
    msg = Ractor.receive
    break if msg == :stop
    op, k, v, rp = msg
    case op
    when :put then db[k] = v; rp << :ok
    when :del then rp << (db.delete(k) ? :deleted : :missing)
    when :size then rp << db.size
    end
  end
  GC.stress = false
  done << :done
  db.size
end
GC.stress = true if STRESS
rp = Ractor::Port.new
model = {}
rng = Random.new(20260722)
100.times do |i|
  k = "k#{rng.rand(50)}"
  if rng.rand(3) == 0
    svc.send([:del, k, nil, rp])
    want = model.delete(k) ? :deleted : :missing
    raise "del#{i}" unless rp.receive == want
  else
    v = i
    model[k] = v
    svc.send([:put, k, v, rp])
    raise "put#{i}" unless rp.receive == :ok
  end
  if i % 50 == 49
    GC.start
    svc.send([:size, nil, nil, rp])
    raise "size#{i}" unless rp.receive == model.size
  end
end
GC.stress = false
svc.send(:stop)
done.receive
raise unless svc.value == model.size
puts "OK d07_kv_delete_size"
