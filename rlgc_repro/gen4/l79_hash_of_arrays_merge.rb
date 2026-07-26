# hash-of-arrays payload copied then reduced; compact
# axes: copy, hash of arrays, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
w = Ractor.new(port) { |o| g = Ractor.receive; o.send(g.values.reduce(:+).sum) }
g = {}; 30.times { |i| g["k#{i}"] = Array.new(5) { |j| i + j } }
exp = g.values.reduce(:+).sum
w.send(g, move: false)
GC.compact
res = port.receive; w.value
raise "mrg #{res}!=#{exp}" unless res == exp
puts "OK l79_hash_of_arrays_merge"
