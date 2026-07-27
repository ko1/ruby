# streaming top-8 を sorted-insert で維持する worker (heap 代替) と sort 参照比較
# axes: 1 worker, copy, fixed-size candidate list
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 60
K = 8
vals = Array.new(N) { |i| (i * 137 + 29) % 331 }
exp = vals.sort.last(K).sort

out = Ractor::Port.new
w = Ractor.new(out, K) do |o, k|
  top = []
  while (v = Ractor.receive) != :eof
    if top.size < k
      top << v
      top.sort!
    elsif v > top[0]
      top[0] = v
      top.sort!
    end
  end
  o.send(top)
end
vals.each { |v| w.send(v) }
w.send(:eof)
got = out.receive
w.value
raise "top=#{got} exp=#{exp}" unless got == exp
puts "OK b59_topk_stream_insert"
