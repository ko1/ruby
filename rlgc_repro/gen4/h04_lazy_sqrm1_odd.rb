# h04_lazy_sqrm1_odd: lazy: x*x-1 then reject even, first(k)
# axes: lazy-chain, copy, GC.start, single worker
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
ref = (1..36).map { |x| x * x - 1 }.reject { |y| y % 2 == 0 }.first(6)
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  res = (1..36).lazy.map { |x| x * x - 1 }.reject { |y| y % 2 == 0 }.first(6)
  GC.start
  po.send(res)
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h04_lazy_sqrm1_odd"
