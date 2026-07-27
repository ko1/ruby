# Materialized running-sum view maintained incrementally by deltas; periodically
# compared against a full recompute from the base table service.
# Axes: base + view services, 120 updates, copy, stress in view.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
base = Ractor.new(done) do |done|
  rows = {}
  loop do
    msg = Ractor.receive
    break if msg == :stop
    op, k, v, rp = msg
    case op
    when :upsert
      old = rows[k] || 0
      rows[k] = v
      rp << v - old # delta
    when :scan then rp << rows.values.sum
    end
  end
  done << :done
  rows.size
end
view = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  sum = 0
  loop do
    msg = Ractor.receive
    break if msg == :stop
    op, d, rp = msg
    case op
    when :delta then sum += d
    when :read then rp << sum
    end
  end
  GC.stress = false
  done << :done
  sum
end
rp = Ractor::Port.new
rng = Random.new(67)
120.times do |i|
  k = "row#{rng.rand(25)}"
  v = rng.rand(1000)
  base.send([:upsert, k, v, rp])
  delta = rp.receive
  view.send([:delta, delta, nil])
  if i % 20 == 19
    base.send([:scan, nil, nil, rp])
    full = rp.receive
    view.send([:read, nil, rp])
    raise "view drift@#{i}" unless rp.receive == full
  end
end
base.send(:stop)
view.send(:stop)
2.times { done.receive }
base.value
view.value
puts "OK d67_view_incr_sum"
