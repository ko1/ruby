# Struct records copied; class + fields preserved
# axes: copy, Struct, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
Rec40 = Struct.new(:id, :amt, :name)
port = Ractor::Port.new
w = Ractor.new(port) { |o| rs = Ractor.receive; o.send([rs.first.class.name, rs.sum(&:amt)]) }
recs = Array.new(100) { |i| Rec40.new(i, i * 5, "r#{i}") }
exp = recs.sum(&:amt)
w.send(recs, move: false)
GC.compact
cls, s = port.receive; w.value
raise "cls #{cls}" unless cls == "Rec40"
raise "sum #{s}!=#{exp}" unless s == exp
puts "OK l40_struct_sum_copy"
