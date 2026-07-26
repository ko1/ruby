# h79_comb_uniq_lazy: lazy combinator: lazy map mod uniq first
# axes: lazy-combinator, copy, GC.start, single worker
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def calc_comb_uniq_lazy(n)
  (1..n).lazy.map { |x| x % 5 }.uniq.first(5)
end
ref = calc_comb_uniq_lazy(26)
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  GC.start
  po.send(calc_comb_uniq_lazy(26))
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h79_comb_uniq_lazy"
