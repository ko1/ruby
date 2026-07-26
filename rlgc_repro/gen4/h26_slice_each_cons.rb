# h26_slice_each_cons: each_cons(2) over range
# axes: enumerator-slicing, copy, GC.compact, single worker
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def calc_slice_each_cons(n)
  data = (1..n).to_a
  data.each_cons(2).to_a
end
ref = calc_slice_each_cons(21)
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  GC.compact
  po.send(calc_slice_each_cons(21))
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h26_slice_each_cons"
