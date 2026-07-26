# Data records moved; class + fields preserved
# axes: move, Data, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
Pt46 = Data.define(:x, :y)
port = Ractor::Port.new
w = Ractor.new(port) { |o| ps = Ractor.receive; o.send([ps.first.class.name, ps.sum { |p| p.x + p.y }]) }
pts = Array.new(120) { |i| Pt46.new(x: i, y: i * 2) }
exp = pts.sum { |p| p.x + p.y }
w.send(pts, move: true)
GC.compact
cls, s = port.receive; w.value
raise "cls #{cls}" unless cls == "Pt46"
raise "sum #{s}!=#{exp}" unless s == exp
puts "OK l46_data_sum_move"
