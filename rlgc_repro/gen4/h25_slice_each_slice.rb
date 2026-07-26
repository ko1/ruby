# h25_slice_each_slice: each_slice(3) over range
# axes: enumerator-slicing, copy, GC.compact, single worker
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def calc_slice_each_slice(n)
  data = (1..n).to_a
  data.each_slice(3).to_a
end
ref = calc_slice_each_slice(20)
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  GC.compact
  po.send(calc_slice_each_slice(20))
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h25_slice_each_slice"
