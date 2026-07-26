# big array moved to worker; compact during transfer window
# axes: move, big array(1500), compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
N = 1500
port = Ractor::Port.new
w = Ractor.new(port) { |o| a = Ractor.receive; o.send([a.min, a.max]) }
arr = Array.new(N) { |i| (i * 7 + 3) % 100003 }
exp = [arr.min, arr.max]
w.send(arr, move: true)
GC.compact
res = port.receive; w.value
raise "minmax #{res.inspect}" unless res == exp
puts "OK l02_big_array_move_minmax"
