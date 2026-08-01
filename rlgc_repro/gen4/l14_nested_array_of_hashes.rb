# nested mixed structure copied; compact during window
# axes: copy, nested mixed, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
w = Ractor.new(port) { |o| a = Ractor.receive; o.send(a.sum { |h| h[:v] }) }
a = Array.new(200) { |i| { id: i, v: i * 2, name: "n#{i}" } }
exp = a.sum { |h| h[:v] }
w.send(a)
GC.compact
res = port.receive; w.value
raise "aoh #{res}!=#{exp}" unless res == exp
puts "OK l14_nested_array_of_hashes"
