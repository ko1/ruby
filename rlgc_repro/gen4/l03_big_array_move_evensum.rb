# big array moved to worker; compact during transfer window
# axes: move, big array(1200), compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
N = 1200
port = Ractor::Port.new
w = Ractor.new(port) { |o| a = Ractor.receive; o.send(a.select(&:even?).sum) }
arr = Array.new(N) { |i| i }
exp = (0...N).select(&:even?).sum
w.send(arr, move: true)
GC.compact
res = port.receive; w.value
raise "evensum #{res}!=#{exp}" unless res == exp
puts "OK l03_big_array_move_evensum"
