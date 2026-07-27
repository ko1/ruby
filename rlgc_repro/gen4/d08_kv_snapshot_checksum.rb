# KV service snapshot op: returns full copied db; main checksums vs local model.
# Axes: 1 service, 100 puts + 5 snapshots, copy of whole hash, stress in service.
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
    when :snap then rp << db.dup
    end
  end
  GC.stress = false
  done << :done
  db.size
end
checksum = ->(h) { h.sort.inject(7) { |a, (k, v)| (a * 31 + k.sum + v.hash) & 0xffffffff } }
rp = Ractor::Port.new
model = {}
100.times do |i|
  k = "s#{i % 30}"
  v = [i, "payload#{i}"]
  model[k] = v
  svc.send([:put, k, v, rp])
  raise unless rp.receive == :ok
  if i % 20 == 19
    svc.send([:snap, nil, nil, rp])
    snap = rp.receive
    raise "snap@#{i}" unless snap == model && checksum.call(snap) == checksum.call(model)
  end
end
svc.send(:stop)
done.receive
raise unless svc.value == 30
puts "OK d08_kv_snapshot_checksum"
