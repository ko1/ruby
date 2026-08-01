# big string copied; compact during window
# axes: copy, big string(50KB), compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
KB = 50
port = Ractor::Port.new
w = Ractor.new(port) { |o| s = Ractor.receive; o.send(s.upcase[0, 4]) }
s = ("abcd" * (KB * 256)).dup
w.send(s, move: false)
GC.compact
res = port.receive; w.value
raise "up #{res}" unless res == "ABCD"
puts "OK l12_big_string_copy_upcase"
