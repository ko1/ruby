# 部分ヒストグラム (Array) を move で main に回収して加算 (worker 側 stress 付き)
# axes: 2 workers, move partials, worker-side GC.stress, tiny load
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 20
vals = Array.new(N) { |i| (i * 13) % 40 }
exp = Array.new(4, 0)
vals.each { |v| exp[v / 10] += 1 }

out = Ractor::Port.new
ws = 2.times.map do |wi|
  Ractor.new(out, vals[wi * (N / 2), N / 2]) do |o, part|
    GC.stress = true if ENV['S_STRESS']
    h = Array.new(4, 0)
    part.each { |v| h[v / 10] += 1 }
    GC.stress = false
    o.send(h, move: true)
  end
end
got = Array.new(4, 0)
2.times { out.receive.each_with_index { |c, i| got[i] += c } }
ws.each(&:value)
raise "hist=#{got} exp=#{exp}" unless got == exp
puts "OK b66_hist_move_partials"
