# tumbling window (幅 8) の合計列を streaming worker が出力し厳密比較
# axes: 1 worker stream, copy, window flush per 8 events
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 48
W = 8
vals = Array.new(N) { |i| (i * 31) % 100 }
exp = vals.each_slice(W).map(&:sum)

out = Ractor::Port.new
w = Ractor.new(out, W) do |o, width|
  acc = 0
  cnt = 0
  while (v = Ractor.receive) != :eof
    acc += v
    cnt += 1
    if cnt == width
      o.send(acc)
      acc = 0
      cnt = 0
    end
  end
  o.send(acc) if cnt > 0
  o.send(:done)
end
vals.each { |v| w.send(v) }
w.send(:eof)
got = []
while (m = out.receive) != :done
  got << m
end
w.value
raise "windows=#{got} exp=#{exp}" unless got == exp
puts "OK b47_window_tumbling"
