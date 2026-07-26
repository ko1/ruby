# big array moved to worker; compact during transfer window
# axes: move, big array(1200), compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
N = 1200
port = Ractor::Port.new
w = Ractor.new(port) { |o| a = Ractor.receive; o.send([a.first, a.last, a.length]) }
arr = Array.new(N) { |i| i * 2 }
exp = [arr.first, arr.last, arr.length]
w.send(arr, move: true)
GC.compact
res = port.receive; w.value
raise "rev #{res.inspect}" unless res == exp
puts "OK l04_big_array_move_reverse"
