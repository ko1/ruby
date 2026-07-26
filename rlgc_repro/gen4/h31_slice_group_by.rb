# h31_slice_group_by: group_by x%4
# axes: enumerator-slicing, copy, GC.compact, single worker
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def calc_slice_group_by(n)
  data = (1..n).to_a
  data.group_by { |x| x % 4 }
end
ref = calc_slice_group_by(26)
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  GC.compact
  po.send(calc_slice_group_by(26))
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h31_slice_group_by"
