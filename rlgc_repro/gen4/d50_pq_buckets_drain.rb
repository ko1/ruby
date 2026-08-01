# 3-level bucket priority queue with interleaved push/pop; pop takes highest
# non-empty bucket; sequence vs model replay. Axes: 150 ops, copy, stress svc.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  buckets = { high: [], mid: [], low: [] }
  loop do
    msg = Ractor.receive
    break if msg == :stop
    op, lvl, v, rp = msg
    case op
    when :push then buckets[lvl] << v; rp << :ok
    when :pop
      src = %i[high mid low].find { |l| !buckets[l].empty? }
      rp << (src ? [src, buckets[src].shift] : :empty)
    end
  end
  GC.stress = false
  done << :done
  buckets.transform_values(&:size)
end
rp = Ractor::Port.new
mb = { high: [], mid: [], low: [] }
rng = Random.new(50)
lvls = %i[high mid low]
150.times do |i|
  if rng.rand(3) < 2
    lvl = lvls[rng.rand(3)]
    mb[lvl] << i
    svc.send([:push, lvl, i, rp])
    raise unless rp.receive == :ok
  else
    src = lvls.find { |l| !mb[l].empty? }
    want = src ? [src, mb[src].shift] : :empty
    svc.send([:pop, nil, nil, rp])
    raise "pop#{i}" unless rp.receive == want
  end
end
svc.send(:stop)
done.receive
raise "leftover" unless svc.value == mb.transform_values(&:size)
puts "OK d50_pq_buckets_drain"
