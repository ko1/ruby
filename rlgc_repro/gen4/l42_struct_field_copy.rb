# Struct records copied; class + fields preserved
# axes: copy, Struct, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
Rec42 = Struct.new(:id, :amt, :name)
port = Ractor::Port.new
w = Ractor.new(port) { |o| r = Ractor.receive; o.send([r.class.name, r.id, r.amt, r.name]) }
r = Rec42.new(7, 700, "seven")
w.send(r, move: false)
GC.compact
cls, id, amt, name = port.receive; w.value
raise unless cls == "Rec42" && id == 7 && amt == 700 && name == "seven"
puts "OK l42_struct_field_copy"
