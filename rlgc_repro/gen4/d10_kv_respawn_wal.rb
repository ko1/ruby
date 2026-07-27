# KV crash+respawn: service keeps WAL; on :crash returns WAL via port, main
# respawns a new service that rebuilds state by replaying the WAL. Axes: 2 gens,
# 120 ops, copy, stress in services.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
spawn = lambda do |wal|
  Ractor.new(done, wal, STRESS) do |done, wal, stress|
    GC.stress = true if stress
    db = {}
    log = []
    wal.each { |k, v| db[k] = v; log << [k, v] }
    loop do
      msg = Ractor.receive
      break if msg == :stop
      op, k, v, rp = msg
      case op
      when :put then db[k] = v; log << [k, v]; rp << :ok
      when :get then rp << db[k]
      when :crash
        GC.stress = false
        rp << log
        break
      end
    end
    GC.stress = false
    done << :done
    db.size
  end
end
rp = Ractor::Port.new
model = {}
svc = spawn.call([])
60.times do |i|
  k = "k#{i % 25}"
  model[k] = i
  svc.send([:put, k, i, rp])
  raise unless rp.receive == :ok
end
svc.send([:crash, nil, nil, rp])
wal = rp.receive
done.receive
raise unless svc.value == 25
svc = spawn.call(wal)
60.times do |i|
  j = i + 60
  k = "k#{j % 25}"
  model[k] = j
  svc.send([:put, k, j, rp])
  raise unless rp.receive == :ok
end
model.each do |k, v|
  svc.send([:get, k, nil, rp])
  raise "rebuild #{k}" unless rp.receive == v
end
svc.send(:stop)
done.receive
raise unless svc.value == model.size
puts "OK d10_kv_respawn_wal"
