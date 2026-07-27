# first-seen 順を保存する streaming dedup を worker で行い順序ごと厳密比較
# axes: 1 worker, copy, order-preserving dedup
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 50
keys = Array.new(N) { |i| (i * 7 + i / 3) % 21 }
exp = keys.uniq

out = Ractor::Port.new
w = Ractor.new(out) do |o|
  seen = {}
  order = []
  while (k = Ractor.receive) != :eof
    unless seen.key?(k)
      seen[k] = true
      order << k
    end
  end
  o.send(order)
end
keys.each { |k| w.send(k) }
w.send(:eof)
got = out.receive
w.value
raise "order=#{got} exp=#{exp}" unless got == exp
puts "OK b57_dedup_firstseen"
