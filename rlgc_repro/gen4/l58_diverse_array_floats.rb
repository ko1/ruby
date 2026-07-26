# structurally diverse array payload; compact during window
# axes: copy/move, diverse array, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
w = Ractor.new(port) { |o| a = Ractor.receive; o.send(a.sum.round(2)) }
a = Array.new(300) { |i| i * 0.25 }
exp = a.sum.round(2)
w.send(a, move: true)
GC.compact
res = port.receive; w.value
raise "flt #{res}!=#{exp}" unless res == exp
puts "OK l58_diverse_array_floats"
