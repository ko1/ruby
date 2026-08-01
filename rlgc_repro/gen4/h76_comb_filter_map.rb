# h76_comb_filter_map: lazy combinator: lazy filter_map squares of odd
# axes: lazy-combinator, copy, GC.start, single worker
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def calc_comb_filter_map(n)
  (1..n).lazy.filter_map { |x| x * x if x.odd? }.first(6)
end
ref = calc_comb_filter_map(23)
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  GC.start
  po.send(calc_comb_filter_map(23))
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h76_comb_filter_map"
