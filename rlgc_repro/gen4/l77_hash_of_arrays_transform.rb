# hash-of-arrays payload moved then reduced; compact
# axes: move, hash of arrays, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
w = Ractor.new(port) { |o| g = Ractor.receive; o.send(g.transform_values { |a| a.map { |x| x * 2 } }["k0"].sum) }
g = {}; 40.times { |i| g["k#{i}"] = Array.new(8) { |j| i * 8 + j } }
exp = (0...8).map { |j| j * 2 }.sum
w.send(g, move: true)
GC.compact
res = port.receive; w.value
raise "tv #{res}!=#{exp}" unless res == exp
puts "OK l77_hash_of_arrays_transform"
