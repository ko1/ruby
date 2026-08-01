# Struct records copied; class + fields preserved
# axes: copy, Struct, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
Inner44 = Struct.new(:x, :y)
Outer44 = Struct.new(:tag, :inner)
port = Ractor::Port.new
w = Ractor.new(port) { |o| r = Ractor.receive; o.send([r.class.name, r.inner.class.name, r.inner.x + r.inner.y]) }
r = Outer44.new("t", Inner44.new(30, 12))
w.send(r, move: false)
GC.compact
oc, ic, s = port.receive; w.value
raise unless oc == "Outer44" && ic == "Inner44" && s == 42
puts "OK l44_struct_nested_copy"
