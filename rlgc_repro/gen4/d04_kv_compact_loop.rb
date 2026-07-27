# KV service running GC.compact inside its request loop every 16 requests.
# Axes: 1 shard, 150 mixed ops, copy, GC.compact in service, stress in service.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  db = {}
  n = 0
  loop do
    msg = Ractor.receive
    break if msg == :stop
    n += 1
    GC.compact if n % 16 == 0
    op, k, v, rp = msg
    case op
    when :put then db[k] = v; rp << :ok
    when :get then rp << db[k]
    end
  end
  GC.stress = false
  done << :done
  db.size
end
rp = Ractor::Port.new
model = {}
150.times do |i|
  if i % 3 == 2
    k = "k#{i % 40}"
    svc.send([:get, k, nil, rp])
    raise "get#{i}" unless rp.receive == model[k]
  else
    k = "k#{i % 40}"
    v = ["val", i, [i, i + 1]]
    model[k] = v
    svc.send([:put, k, v, rp])
    raise "put#{i}" unless rp.receive == :ok
  end
end
svc.send(:stop)
done.receive
raise unless svc.value == model.size
puts "OK d04_kv_compact_loop"
