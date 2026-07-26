# h78_comb_tally: lazy combinator: tally of mod values
# axes: lazy-combinator, copy, GC.start, single worker
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def calc_comb_tally(n)
  (1..n).map { |x| x % 4 }.tally
end
ref = calc_comb_tally(25)
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  GC.start
  po.send(calc_comb_tally(25))
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h78_comb_tally"
