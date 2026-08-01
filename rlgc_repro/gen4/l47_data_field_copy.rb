# Data records copied; class + fields preserved
# axes: copy, Data, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
Pt47 = Data.define(:x, :y)
port = Ractor::Port.new
w = Ractor.new(port) { |o| p = Ractor.receive; o.send([p.class.name, p.x, p.y]) }
p = Pt47.new(x: 11, y: 22)
w.send(p, move: false)
GC.compact
cls, x, y = port.receive; w.value
raise unless cls == "Pt47" && x == 11 && y == 22
puts "OK l47_data_field_copy"
