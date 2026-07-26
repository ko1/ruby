# big hash moved to worker; compact mid-transfer
# axes: move, big hash(500), compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
N = 500
port = Ractor::Port.new
w = Ractor.new(port) { |o| h = Ractor.receive; o.send(h.size) }
h = {}; N.times { |i| h[i] = "v#{i}" }
w.send(h, move: true)
GC.compact
res = port.receive; w.value
raise "size #{res}" unless res == N
puts "OK l06_big_hash_move_keycount"
