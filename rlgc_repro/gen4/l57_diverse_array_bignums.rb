# structurally diverse array payload; compact during window
# axes: copy/move, diverse array, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
w = Ractor.new(port) { |o| a = Ractor.receive; o.send(a.sum) }
a = Array.new(120) { |i| 10**30 + i }
exp = a.sum
w.send(a, move: true)
GC.compact
res = port.receive; w.value
raise "big #{res}!=#{exp}" unless res == exp
puts "OK l57_diverse_array_bignums"
