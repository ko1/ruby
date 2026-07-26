# Array subclass moved; class preserved on move
# axes: move, Array subclass, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
class MyArr19 < Array; end
port = Ractor::Port.new
w = Ractor.new(port) { |o| a = Ractor.receive; o.send([a.class.name, a.length]) }
a = MyArr19.new
100.times { |i| a << i }
w.send(a, move: true)
GC.compact
cls, val = port.receive; w.value
raise "cls #{cls}" unless cls == "MyArr19"
raise "val #{val}" unless val == (100)
puts "OK l19_subclass_array_move_len"
