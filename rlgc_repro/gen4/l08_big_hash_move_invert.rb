# big hash moved to worker; compact mid-transfer
# axes: move, big hash(500), compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
N = 500
port = Ractor::Port.new
w = Ractor.new(port) { |o| h = Ractor.receive; inv = h.invert; o.send(inv[N-1]) }
h = {}; N.times { |i| h[i] = N - 1 - i }
w.send(h, move: true)
GC.compact
res = port.receive; w.value
raise "invert #{res}" unless res == 0
puts "OK l08_big_hash_move_invert"
