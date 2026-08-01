# h01_lazy_sqr_even: lazy: square then keep even, first(k)
# axes: lazy-chain, copy, GC.start, single worker
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
ref = (1..30).map { |x| x * x }.select { |y| y % 2 == 0 }.first(6)
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  res = (1..30).lazy.map { |x| x * x }.select { |y| y % 2 == 0 }.first(6)
  GC.start
  po.send(res)
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h01_lazy_sqr_even"
