# window flush ごとに worker が GC.start / 2 回に 1 回 GC.compact する集計
# axes: 1 worker, copy, GC 呼び出しを window 境界に同期
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 32
W = 8
vals = Array.new(N) { |i| (i * 23) % 70 }
exp = vals.each_slice(W).map(&:sum)

out = Ractor::Port.new
w = Ractor.new(out, W) do |o, width|
  buf = []
  nw = 0
  while (v = Ractor.receive) != :eof
    buf << v
    if buf.size == width
      o.send(buf.sum)
      buf = []
      nw += 1
      nw.odd? ? GC.start : GC.compact
    end
  end
  o.send(:done)
end
vals.each { |v| w.send(v) }
w.send(:eof)
got = []
while (m = out.receive) != :done
  got << m
end
w.value
raise "wins=#{got} exp=#{exp}" unless got == exp
puts "OK b54_window_gc_flush"
