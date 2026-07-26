# h74_comb_chunk_while: lazy combinator: chunk_while ascending runs
# axes: lazy-combinator, copy, GC.start, single worker
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def calc_comb_chunk_while(n)
  [1, 2, 3, 1, 2, 5, 6, 1].chunk_while { |a, b| b > a }.to_a
end
ref = calc_comb_chunk_while(21)
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  GC.start
  po.send(calc_comb_chunk_while(21))
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h74_comb_chunk_while"
