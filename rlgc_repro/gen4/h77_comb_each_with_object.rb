# h77_comb_each_with_object: lazy combinator: each_with_object histogram
# axes: lazy-combinator, copy, GC.start, single worker
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def calc_comb_each_with_object(n)
  (1..n).each_with_object(Hash.new(0)) { |x, h| h[x % 3] += 1 }
end
ref = calc_comb_each_with_object(24)
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  GC.start
  po.send(calc_comb_each_with_object(24))
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h77_comb_each_with_object"
