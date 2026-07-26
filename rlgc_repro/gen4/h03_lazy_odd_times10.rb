# h03_lazy_odd_times10: lazy: keep odd then *10, first(k)
# axes: lazy-chain, copy, GC.start, single worker
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
ref = (1..34).select { |x| x % 2 == 1 }.map { |x| x * 10 }.first(5)
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  res = (1..34).lazy.select { |x| x % 2 == 1 }.map { |x| x * 10 }.first(5)
  GC.start
  po.send(res)
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h03_lazy_odd_times10"
