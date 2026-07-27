# 2-pass: pass1 で min/max を並列集計 -> shareable bounds -> pass2 で 4 分位 bucket 数え
# axes: 2 workers x 2 passes (respawn), shareable bounds table
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 40
vals = Array.new(N) { |i| (i * 173) % 1000 }
# pass1: min/max
p1 = Ractor::Port.new
w1 = 2.times.map do |k|
  Ractor.new(p1, vals[k * (N / 2), N / 2], k) do |o, part, _id|
    o.send([part.min, part.max])
  end
end
mn = 1 << 30
mx = -1
2.times do
  a, b = p1.receive
  mn = a if a < mn
  mx = b if b > mx
end
w1.each(&:value)
raise "minmax" unless mn == vals.min && mx == vals.max
span = mx - mn + 1
BOUNDS = Ractor.make_shareable([mn + span / 4, mn + span / 2, mn + 3 * span / 4])
exp = [0, 0, 0, 0]
vals.each { |v| exp[BOUNDS.count { |b| v >= b }] += 1 }
# pass2: bucket counts
p2 = Ractor::Port.new
w2 = 2.times.map do |k|
  Ractor.new(p2, vals[k * (N / 2), N / 2], BOUNDS) do |o, part, bounds|
    c = [0, 0, 0, 0]
    part.each { |v| c[bounds.count { |b| v >= b }] += 1 }
    o.send(c)
  end
end
got = [0, 0, 0, 0]
2.times { p2.receive.each_with_index { |c, i| got[i] += c } }
w2.each(&:value)
raise "buckets=#{got} exp=#{exp}" unless got == exp
raise "total" unless got.sum == N
puts "OK b26_csv_two_pass_minmax"
