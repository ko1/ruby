# Stress-focused tiny KV: minimal volume, service also GC.starts every 8 requests
# so plain runs still exercise GC heavily. Axes: 40 ops, copy, stress both sides.
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
    GC.start if n % 8 == 0
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
GC.stress = true if STRESS
rp = Ractor::Port.new
20.times do |i|
  svc.send([:put, "t#{i}", { n: i, s: "str#{i}" }, rp])
  raise unless rp.receive == :ok
end
20.times do |i|
  svc.send([:get, "t#{i}", nil, rp])
  raise "g#{i}" unless rp.receive == { n: i, s: "str#{i}" }
end
GC.stress = false
svc.send(:stop)
done.receive
raise unless svc.value == 20
puts "OK d76_kv_stress_tiny"
