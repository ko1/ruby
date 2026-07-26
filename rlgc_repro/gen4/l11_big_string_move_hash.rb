# big string moved; compact during window
# axes: move, big string(100KB), compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
KB = 100
port = Ractor::Port.new
w = Ractor.new(port) { |o| s = Ractor.receive; o.send(s.sum) }
s = ("ABCDEFGHIJKLMNOPQRSTUVWXYZ" * (KB * 40)).b
exp = s.sum
w.send(s, move: true)
GC.compact
res = port.receive; w.value
raise "sum #{res}!=#{exp}" unless res == exp
puts "OK l11_big_string_move_hash"
