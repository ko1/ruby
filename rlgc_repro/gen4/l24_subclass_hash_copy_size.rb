# Hash subclass copied; class preserved
# axes: copy, Hash subclass, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
class MyHash24 < Hash; end
port = Ractor::Port.new
w = Ractor.new(port) { |o| h = Ractor.receive; o.send([h.class.name, h.size]) }
h = MyHash24.new
50.times { |i| h[i] = i }
w.send(h, move: false)
GC.compact
cls, val = port.receive; w.value
raise "cls #{cls}" unless cls == "MyHash24"
raise unless val == 50
puts "OK l24_subclass_hash_copy_size"
