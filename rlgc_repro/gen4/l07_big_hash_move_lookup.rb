# big hash moved to worker; compact mid-transfer
# axes: move, big hash(500), compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
N = 500
port = Ractor::Port.new
w = Ractor.new(port) { |o| h = Ractor.receive; o.send(h["k#{N/2}"]) }
h = {}; N.times { |i| h["k#{i}"] = i * 3 }
exp = (N/2) * 3
w.send(h, move: true)
GC.compact
res = port.receive; w.value
raise "lookup #{res}!=#{exp}" unless res == exp
puts "OK l07_big_hash_move_lookup"
