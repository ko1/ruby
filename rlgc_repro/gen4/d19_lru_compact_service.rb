# LRU cache service running GC.compact every 12 requests inside its loop.
# Axes: cap=5, 100 ops, copy, stress in service + compact in loop.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  h = {}
  n = 0
  ev = 0
  loop do
    msg = Ractor.receive
    break if msg == :stop
    n += 1
    GC.compact if n % 12 == 0
    op, k, v, rp = msg
    case op
    when :put
      h.delete(k); h[k] = [v, "meta-#{v}"]
      if h.size > 5
        h.delete(h.first[0]); ev += 1
      end
      rp << :ok
    when :get
      rp << (h.key?(k) ? h[k][0] : nil)
    end
  end
  GC.stress = false
  done << :done
  [h.size, ev]
end
rp = Ractor::Port.new
mh = {}
mev = 0
100.times do |i|
  k = "k#{i % 9}"
  if i.even?
    mh.delete(k); mh[k] = i
    if mh.size > 5
      mh.delete(mh.first[0]); mev += 1
    end
    svc.send([:put, k, i, rp])
    raise unless rp.receive == :ok
  else
    svc.send([:get, k, nil, rp])
    raise "get#{i}" unless rp.receive == mh[k]
  end
end
svc.send(:stop)
done.receive
raise unless svc.value == [mh.size, mev]
puts "OK d19_lru_compact_service"
