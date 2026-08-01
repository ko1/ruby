# sliding window (幅 5, step 1) の合計列を streaming worker が出し each_cons と比較
# axes: 1 worker, copy, ring buffer in worker
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 36
W = 5
vals = Array.new(N) { |i| (i * 17 + 3) % 50 }
exp = vals.each_cons(W).map(&:sum)

out = Ractor::Port.new
w = Ractor.new(out, W) do |o, width|
  buf = []
  sums = []
  while (v = Ractor.receive) != :eof
    buf << v
    buf.shift if buf.size > width
    sums << buf.sum if buf.size == width
  end
  o.send(sums)
end
vals.each { |v| w.send(v) }
w.send(:eof)
got = out.receive
w.value
raise "n=#{got.size}" unless got.size == exp.size
raise "sums" unless got == exp
puts "OK b48_window_sliding"
