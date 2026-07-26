# h27_slice_chunk_mod3: chunk by x%3
# axes: enumerator-slicing, copy, GC.compact, single worker
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def calc_slice_chunk_mod3(n)
  data = (1..n).to_a
  data.chunk { |x| x % 3 }.to_a
end
ref = calc_slice_chunk_mod3(22)
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  GC.compact
  po.send(calc_slice_chunk_mod3(22))
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h27_slice_chunk_mod3"
