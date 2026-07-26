# big string copied; compact during window
# axes: copy, big string(60KB), compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
KB = 60
port = Ractor::Port.new
w = Ractor.new(port) { |o| s = Ractor.receive; o.send(s.bytesize) }
s = "ab" * (KB * 512)
exp = s.bytesize
w.send(s, move: false)
GC.compact
res = port.receive; w.value
raise "bytes #{res}!=#{exp}" unless res == exp
puts "OK l10_big_string_copy_bytes"
