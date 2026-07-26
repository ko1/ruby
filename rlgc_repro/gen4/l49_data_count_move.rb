# Data records moved; class + fields preserved
# axes: move, Data, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
Pt49 = Data.define(:x, :y)
port = Ractor::Port.new
w = Ractor.new(port) { |o| ps = Ractor.receive; o.send(ps.size) }
pts = Array.new(120) { |i| Pt49.new(x: i, y: -i) }
w.send(pts, move: true)
GC.compact
c = port.receive; w.value
raise "count #{c}" unless c == 120
puts "OK l49_data_count_move"
