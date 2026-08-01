# Struct records moved; class + fields preserved
# axes: move, Struct, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
Rec43 = Struct.new(:k, :v)
port = Ractor::Port.new
w = Ractor.new(port) { |o| rs = Ractor.receive; o.send(rs.size) }
recs = Array.new(120) { |i| Rec43.new(i, "v#{i}") }
w.send(recs, move: true)
GC.compact
c = port.receive; w.value
raise "count #{c}" unless c == 120
puts "OK l43_struct_count_move"
