# h07_lazy_takewhile: lazy: *2 take_while < N, to_a
# axes: lazy-chain, copy, GC.start, single worker
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
ref = (1..42).map { |x| x * 2 }.take_while { |y| y < 42 }.to_a
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  res = (1..42).lazy.map { |x| x * 2 }.take_while { |y| y < 42 }.to_a
  GC.start
  po.send(res)
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h07_lazy_takewhile"
