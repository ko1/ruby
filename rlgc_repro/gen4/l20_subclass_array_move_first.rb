# Array subclass moved; class preserved on move
# axes: move, Array subclass, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
class MyArr20 < Array; end
port = Ractor::Port.new
w = Ractor.new(port) { |o| a = Ractor.receive; o.send([a.class.name, a.first]) }
a = MyArr20.new
100.times { |i| a << i }
w.send(a, move: true)
GC.compact
cls, val = port.receive; w.value
raise "cls #{cls}" unless cls == "MyArr20"
raise "val #{val}" unless val == (0)
puts "OK l20_subclass_array_move_first"
