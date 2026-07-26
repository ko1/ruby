# Array carrying generic ivar hash copied
# axes: copy, generic ivar Array, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
w = Ractor.new(port) { |o| a = Ractor.receive; o.send([a.sum, a.instance_variable_get(:@meta)]) }
a = Array.new(200) { |i| i }
a.instance_variable_set(:@meta, { owner: "o33", ver: 33 })
exp = (0...200).sum
w.send(a, move: false)
GC.compact
sum, meta = port.receive; w.value
raise "sum #{sum}" unless sum == exp
raise "meta #{meta.inspect}" unless meta == { owner: "o33", ver: 33 }
puts "OK l33_ivar_array_copy"
