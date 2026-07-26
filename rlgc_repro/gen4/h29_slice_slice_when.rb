# h29_slice_slice_when: slice_when at even boundary
# axes: enumerator-slicing, copy, GC.compact, single worker
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def calc_slice_slice_when(n)
  data = (1..n).to_a
  data.slice_when { |a, b| b.even? }.to_a
end
ref = calc_slice_slice_when(24)
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  GC.compact
  po.send(calc_slice_slice_when(24))
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h29_slice_slice_when"
