# h71_comb_flatmap: lazy combinator: flat_map ranges then first
# axes: lazy-combinator, copy, GC.start, single worker
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def calc_comb_flatmap(n)
  (1..n).lazy.flat_map { |x| (1..x) }.first(12)
end
ref = calc_comb_flatmap(18)
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  GC.start
  po.send(calc_comb_flatmap(18))
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h71_comb_flatmap"
