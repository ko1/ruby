# big string moved; compact during window
# axes: move, big string(80KB), compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
KB = 80
port = Ractor::Port.new
w = Ractor.new(port) { |o| s = Ractor.receive; o.send(s.bytesize) }
s = "ab" * (KB * 512)
exp = s.bytesize
w.send(s, move: true)
GC.compact
res = port.receive; w.value
raise "bytes #{res}!=#{exp}" unless res == exp
puts "OK l09_big_string_move_bytes"
