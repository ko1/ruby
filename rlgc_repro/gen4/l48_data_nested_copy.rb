# Data records copied; class + fields preserved
# axes: copy, Data, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
Leaf48 = Data.define(:v)
Tree48 = Data.define(:tag, :leaf)
port = Ractor::Port.new
w = Ractor.new(port) { |o| t = Ractor.receive; o.send([t.class.name, t.leaf.class.name, t.leaf.v]) }
t = Tree48.new(tag: "root", leaf: Leaf48.new(v: 99))
w.send(t, move: false)
GC.compact
tc, lc, v = port.receive; w.value
raise unless tc == "Tree48" && lc == "Leaf48" && v == 99
puts "OK l48_data_nested_copy"
