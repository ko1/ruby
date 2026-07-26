# nested mixed structure copied; compact during window
# axes: copy, nested mixed, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
w = Ractor.new(port) { |o| g = Ractor.receive; o.send(g[:a][:b][:c].sum) }
g = { a: { b: { c: (1..50).to_a } } }
exp = (1..50).sum
w.send(g)
GC.compact
res = port.receive; w.value
raise "deep #{res}!=#{exp}" unless res == exp
puts "OK l13_nested_deep_map"
