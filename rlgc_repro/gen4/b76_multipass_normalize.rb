# pass1 で並列 min 検出 -> pass2 で全値から min を引く正規化 (整数) -> 合計シフト検証
# axes: 2 workers x 2 passes respawn, copy
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 40
vals = Array.new(N) { |i| (i * 71 + 23) % 300 + 50 }
p1 = Ractor::Port.new
w1 = 2.times.map do |wi|
  Ractor.new(p1, vals[wi * (N / 2), N / 2]) { |o, part| o.send(part.min) }
end
mn = [p1.receive, p1.receive].min
w1.each(&:value)
raise "min" unless mn == vals.min

p2 = Ractor::Port.new
w2 = 2.times.map do |wi|
  Ractor.new(p2, vals[wi * (N / 2), N / 2], mn, wi) do |o, part, m, id|
    o.send([id, part.map { |v| v - m }])
  end
end
norm = Array.new(2)
2.times do
  id, a = p2.receive
  norm[id] = a
end
w2.each(&:value)
flat = norm[0] + norm[1]
raise "len" unless flat.size == N
raise "min0" unless flat.min == 0
raise "sum" unless flat.sum == vals.sum - N * mn
puts "OK b76_multipass_normalize"
