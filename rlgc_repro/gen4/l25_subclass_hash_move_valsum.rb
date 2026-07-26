# Hash subclass moved; class preserved
# axes: move, Hash subclass, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
class MyHash25 < Hash; end
port = Ractor::Port.new
w = Ractor.new(port) { |o| h = Ractor.receive; o.send([h.class.name, h.values.sum]) }
h = MyHash25.new
50.times { |i| h[i] = i * 2 }
w.send(h, move: true)
GC.compact
cls, val = port.receive; w.value
raise "cls #{cls}" unless cls == "MyHash25"
raise unless val == (0...50).map{|i|i*2}.sum
puts "OK l25_subclass_hash_move_valsum"
