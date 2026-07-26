# h32_slice_slice_sum: each_slice(4) then sums
# axes: enumerator-slicing, copy, GC.compact, single worker
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def calc_slice_slice_sum(n)
  data = (1..n).to_a
  data.each_slice(4).map { |s| s.sum }
end
ref = calc_slice_slice_sum(27)
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  GC.compact
  po.send(calc_slice_slice_sum(27))
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h32_slice_slice_sum"
