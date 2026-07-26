# hash-of-arrays payload moved then reduced; compact
# axes: move, hash of arrays, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
w = Ractor.new(port) { |o| g = Ractor.receive; o.send(g.sum { |_k, a| a.count(&:even?) }) }
g = {}; 50.times { |i| g[i] = Array.new(10) { |j| i * 10 + j } }
exp = g.sum { |_k, a| a.count(&:even?) }
w.send(g, move: true)
GC.compact
res = port.receive; w.value
raise "flt #{res}!=#{exp}" unless res == exp
puts "OK l78_hash_of_arrays_filter"
