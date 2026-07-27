# merge した histogram から p50/p90 を累積で求め、sort 参照の同定義値と比較
# axes: 2 workers, copy, cumulative percentile
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 50
vals = Array.new(N) { |i| (i * 77 + 5) % 100 }
sorted = vals.sort
# 定義: p番目 = 累積数が ceil(N*p/100) 以上になる最初の bucket 値 (bucket 幅 1)
def pct_from_hist(hist, n, p)
  need = (n * p + 99) / 100
  acc = 0
  hist.each_with_index do |c, v|
    acc += c
    return v if acc >= need
  end
  raise "unreachable"
end
exp50 = sorted[(N * 50 + 99) / 100 - 1]
exp90 = sorted[(N * 90 + 99) / 100 - 1]

out = Ractor::Port.new
ws = 2.times.map do |wi|
  Ractor.new(out, vals[wi * (N / 2), N / 2]) do |o, part|
    h = Array.new(100, 0)
    part.each { |v| h[v] += 1 }
    o.send(h)
  end
end
hist = Array.new(100, 0)
2.times { out.receive.each_with_index { |c, i| hist[i] += c } }
ws.each(&:value)
g50 = pct_from_hist(hist, N, 50)
g90 = pct_from_hist(hist, N, 90)
raise "p50 #{g50} != #{exp50}" unless g50 == exp50
raise "p90 #{g90} != #{exp90}" unless g90 == exp90
puts "OK b68_hist_percentile"
