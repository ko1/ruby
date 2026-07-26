# Hash subclass copied; class preserved
# axes: copy, Hash subclass, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
class MyHash26 < Hash; end
port = Ractor::Port.new
w = Ractor.new(port) { |o| h = Ractor.receive; o.send([h.class.name, h.keys.sort.first]) }
h = MyHash26.new
50.times { |i| h[i] = "v" }
w.send(h, move: false)
GC.compact
cls, val = port.receive; w.value
raise "cls #{cls}" unless cls == "MyHash26"
raise unless val == 0
puts "OK l26_subclass_hash_copy_keys"
