# nested mixed structure copied; compact during window
# axes: copy, nested mixed, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
w = Ractor.new(port) { |o| h = Ractor.receive; o.send(h.values.flatten.sum) }
h = {}; 30.times { |i| h["g#{i}"] = Array.new(10) { |j| i * 10 + j } }
exp = h.values.flatten.sum
w.send(h)
GC.compact
res = port.receive; w.value
raise "hoa #{res}!=#{exp}" unless res == exp
puts "OK l15_nested_hash_of_arrays"
