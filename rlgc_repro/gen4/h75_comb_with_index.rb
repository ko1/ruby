# h75_comb_with_index: lazy combinator: map.with_index offset
# axes: lazy-combinator, copy, GC.start, single worker
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def calc_comb_with_index(n)
  (1..n).each.with_index(1).map { |v, i| v * i }
end
ref = calc_comb_with_index(22)
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  GC.start
  po.send(calc_comb_with_index(22))
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h75_comb_with_index"
