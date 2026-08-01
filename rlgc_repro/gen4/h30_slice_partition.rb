# h30_slice_partition: partition even/odd
# axes: enumerator-slicing, copy, GC.compact, single worker
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def calc_slice_partition(n)
  data = (1..n).to_a
  data.partition { |x| x % 2 == 0 }
end
ref = calc_slice_partition(25)
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  GC.compact
  po.send(calc_slice_partition(25))
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h30_slice_partition"
