# h02_lazy_add_mod3: lazy: +3 then multiples of 3, first(k)
# axes: lazy-chain, copy, GC.start, single worker
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
ref = (1..32).map { |x| x + 3 }.select { |y| y % 3 == 0 }.first(7)
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  res = (1..32).lazy.map { |x| x + 3 }.select { |y| y % 3 == 0 }.first(7)
  GC.start
  po.send(res)
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h02_lazy_add_mod3"
