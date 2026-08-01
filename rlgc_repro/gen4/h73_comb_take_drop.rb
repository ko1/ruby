# h73_comb_take_drop: lazy combinator: drop_while then take
# axes: lazy-combinator, copy, GC.start, single worker
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def calc_comb_take_drop(n)
  (1..n).lazy.drop_while { |x| x < 5 }.first(6)
end
ref = calc_comb_take_drop(20)
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  GC.start
  po.send(calc_comb_take_drop(20))
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h73_comb_take_drop"
