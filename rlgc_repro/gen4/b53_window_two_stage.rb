# 2 段 window: stage1 が幅 4 tumbling、stage2 が stage1 出力を幅 3 で再集計
# axes: 2 chained workers, copy, nested windows
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 48
vals = Array.new(N) { |i| (i * 11 + 1) % 60 }
s1 = vals.each_slice(4).map(&:sum)
exp = s1.each_slice(3).map(&:sum)

out = Ractor::Port.new
stage2 = Ractor.new(out) do |o|
  buf = []
  wins = []
  while (v = Ractor.receive) != :eof
    buf << v
    if buf.size == 3
      wins << buf.sum
      buf = []
    end
  end
  wins << buf.sum unless buf.empty?
  o.send(wins)
end
stage1 = Ractor.new(stage2) do |dst|
  buf = []
  while (v = Ractor.receive) != :eof
    buf << v
    if buf.size == 4
      dst.send(buf.sum)
      buf = []
    end
  end
  dst.send(buf.sum) unless buf.empty?
  dst.send(:eof)
end
vals.each { |v| stage1.send(v) }
stage1.send(:eof)
got = out.receive
stage1.value
stage2.value
raise "wins=#{got} exp=#{exp}" unless got == exp
puts "OK b53_window_two_stage"
