# h80_comb_slice_when_map: lazy combinator: slice_when then map size
# axes: lazy-combinator, copy, GC.start, single worker
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def calc_comb_slice_when_map(n)
  (1..n).slice_when { |a, b| b % 4 == 0 }.map(&:size)
end
ref = calc_comb_slice_when_map(27)
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  GC.start
  po.send(calc_comb_slice_when_map(27))
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h80_comb_slice_when_map"
