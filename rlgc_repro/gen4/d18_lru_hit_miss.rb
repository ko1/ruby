# LRU session cache: exact hit/miss accounting vs deterministic access script.
# Axes: cap=6, 150 ops, copy, stress in service, GC.start client scattered.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  cap = 6
  h = {}
  hits = misses = 0
  loop do
    msg = Ractor.receive
    break if msg == :stop
    op, k, v, rp = msg
    case op
    when :put
      h.delete(k); h[k] = v
      h.delete(h.first[0]) if h.size > cap
      rp << :ok
    when :get
      if h.key?(k)
        hits += 1
        val = h.delete(k); h[k] = val
        rp << val
      else
        misses += 1
        rp << nil
      end
    end
  end
  GC.stress = false
  done << :done
  [hits, misses]
end
rp = Ractor::Port.new
# model with identical algorithm
mh = {}
mhits = mmiss = 0
rng = Random.new(18)
150.times do |i|
  k = "u#{rng.rand(10)}"
  if i % 3 == 0
    mh.delete(k); mh[k] = i
    mh.delete(mh.first[0]) if mh.size > 6
    svc.send([:put, k, i, rp])
    raise unless rp.receive == :ok
  else
    if mh.key?(k)
      mhits += 1
      mv = mh.delete(k); mh[k] = mv
    else
      mmiss += 1
      mv = nil
    end
    svc.send([:get, k, nil, rp])
    raise "get#{i}" unless rp.receive == mv
  end
  GC.start if i % 60 == 59
end
svc.send(:stop)
done.receive
raise "hit/miss" unless svc.value == [mhits, mmiss]
puts "OK d18_lru_hit_miss"
