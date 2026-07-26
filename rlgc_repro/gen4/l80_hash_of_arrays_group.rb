# hash-of-arrays payload moved then reduced; compact
# axes: move, hash of arrays, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
w = Ractor.new(port) { |o| g = Ractor.receive; o.send(g.keys.size) }
g = {}; 60.times { |i| g["bucket#{i % 12}"] ||= []; g["bucket#{i % 12}"] << i }
w.send(g, move: true)
GC.compact
res = port.receive; w.value
raise "grp #{res}" unless res == 12
puts "OK l80_hash_of_arrays_group"
