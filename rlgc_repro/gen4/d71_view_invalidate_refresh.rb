# Cached view with explicit invalidation: version bumps on refresh only; stale
# reads allowed until :invalidate; version/value sequencing exact.
# Axes: base + cached view, 6 rounds x 20 updates, copy, stress in view.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
base = Ractor.new(done) do |done|
  vals = Hash.new(0)
  loop do
    msg = Ractor.receive
    break if msg == :stop
    op, k, d, rp = msg
    case op
    when :bump then vals[k] += d; rp << :ok
    when :sum then rp << vals.values.sum
    end
  end
  done << :done
  vals.values.sum
end
view = Ractor.new(base, done, STRESS) do |base, done, stress|
  GC.stress = true if stress
  my = Ractor::Port.new
  cached = 0
  version = 0
  base.send([:sum, nil, nil, my])
  cached = my.receive
  loop do
    msg = Ractor.receive
    break if msg == :stop
    op, rp = msg
    case op
    when :read then rp << [version, cached]
    when :invalidate
      base.send([:sum, nil, nil, my])
      cached = my.receive
      version += 1
      rp << version
    end
  end
  GC.stress = false
  done << :done
  [version, cached]
end
rp = Ractor::Port.new
rng = Random.new(71)
total = 0
6.times do |round|
  view.send([:read, rp])
  ver, cval = rp.receive
  raise "stale ver" unless ver == round
  raise "stale val" unless cval == (round == 0 ? 0 : total)
  20.times do
    d = rng.rand(1..9)
    total += d
    base.send([:bump, "k#{rng.rand(5)}", d, rp])
    raise unless rp.receive == :ok
  end
  view.send([:read, rp])
  ver2, cval2 = rp.receive
  raise "changed w/o invalidate" unless [ver2, cval2] == [ver, cval]
  view.send([:invalidate, rp])
  raise "ver bump" unless rp.receive == round + 1
  view.send([:read, rp])
  raise "fresh" unless rp.receive == [round + 1, total]
end
view.send(:stop)
base.send(:stop)
2.times { done.receive }
raise unless view.value == [6, total] && base.value == total
puts "OK d71_view_invalidate_refresh"
