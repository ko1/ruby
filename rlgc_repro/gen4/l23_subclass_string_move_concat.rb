# String subclass moved; class preserved on move
# axes: move, String subclass, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
class MyStr23 < String; end
port = Ractor::Port.new
w = Ractor.new(port) { |o| s = Ractor.receive; o.send([s.class.name, s.dup.concat("bar")]) }
s = MyStr23.new("foo")
w.send(s, move: true)
GC.compact
cls, val = port.receive; w.value
raise "cls #{cls}" unless cls == "MyStr23"
raise unless val == "foobar"
puts "OK l23_subclass_string_move_concat"
