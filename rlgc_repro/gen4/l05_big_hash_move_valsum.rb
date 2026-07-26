# big hash moved to worker; compact mid-transfer
# axes: move, big hash(500), compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
N = 500
port = Ractor::Port.new
w = Ractor.new(port) { |o| h = Ractor.receive; o.send(h.values.sum) }
h = {}; N.times { |i| h["k#{i}"] = i }
exp = (0...N).sum
w.send(h, move: true)
GC.compact
res = port.receive; w.value
raise "valsum #{res}!=#{exp}" unless res == exp
puts "OK l05_big_hash_move_valsum"
