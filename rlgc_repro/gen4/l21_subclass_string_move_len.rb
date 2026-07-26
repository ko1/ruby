# String subclass moved; class preserved on move
# axes: move, String subclass, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
class MyStr21 < String; end
port = Ractor::Port.new
w = Ractor.new(port) { |o| s = Ractor.receive; o.send([s.class.name, s.length]) }
s = MyStr21.new("abcdefghijklmnopqrstuvwxyz")
w.send(s, move: true)
GC.compact
cls, val = port.receive; w.value
raise "cls #{cls}" unless cls == "MyStr21"
raise unless val == 26
puts "OK l21_subclass_string_move_len"
