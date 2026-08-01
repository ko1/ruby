# Data records copied; class + fields preserved
# axes: copy, Data, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
Pt45 = Data.define(:x, :y)
port = Ractor::Port.new
w = Ractor.new(port) { |o| ps = Ractor.receive; o.send([ps.first.class.name, ps.sum { |p| p.x + p.y }]) }
pts = Array.new(120) { |i| Pt45.new(x: i, y: i * 2) }
exp = pts.sum { |p| p.x + p.y }
w.send(pts, move: false)
GC.compact
cls, s = port.receive; w.value
raise "cls #{cls}" unless cls == "Pt45"
raise "sum #{s}!=#{exp}" unless s == exp
puts "OK l45_data_sum_copy"
