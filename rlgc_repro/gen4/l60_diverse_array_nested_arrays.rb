# structurally diverse array payload; compact during window
# axes: copy/move, diverse array, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
w = Ractor.new(port) { |o| a = Ractor.receive; o.send(a.flatten.sum) }
a = Array.new(30) { |i| Array.new(15) { |j| i * 15 + j } }
exp = a.flatten.sum
w.send(a, move: true)
GC.compact
res = port.receive; w.value
raise "nest #{res}!=#{exp}" unless res == exp
puts "OK l60_diverse_array_nested_arrays"
