# 10 bucket 固定ヒストグラムを 4 worker で部分集計し merge、算術期待値と比較
# axes: 4 workers, copy slices, Array bucket counts
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 80
NW = 4
vals = Array.new(N) { |i| (i * 37 + 11) % 100 }
exp = Array.new(10, 0)
vals.each { |v| exp[v / 10] += 1 }

out = Ractor::Port.new
ws = NW.times.map do
  Ractor.new(out) do |o|
    h = Array.new(10, 0)
    loop do
      sl = Ractor.receive
      break if sl == :stop
      sl.each { |v| h[v / 10] += 1 }
    end
    o.send(h)
  end
end
vals.each_slice(10).with_index { |sl, i| ws[i % NW].send(sl) }
ws.each { |w| w.send(:stop) }
got = Array.new(10, 0)
NW.times { out.receive.each_with_index { |c, i| got[i] += c } }
ws.each(&:value)
raise "hist=#{got} exp=#{exp}" unless got == exp
raise "total" unless got.sum == N
puts "OK b63_hist_fixed_buckets"
