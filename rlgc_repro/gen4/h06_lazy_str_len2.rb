# h06_lazy_str_len2: lazy: to_s then two-digit strings, first(k)
# axes: lazy-chain, copy, GC.start, single worker
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
ref = (1..40).map { |x| x.to_s }.select { |s| s.size == 2 }.first(4)
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  res = (1..40).lazy.map { |x| x.to_s }.select { |s| s.size == 2 }.first(4)
  GC.start
  po.send(res)
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h06_lazy_str_len2"
