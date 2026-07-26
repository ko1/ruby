# structurally diverse array payload; compact during window
# axes: copy/move, diverse array, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
w = Ractor.new(port) { |o| a = Ractor.receive; o.send(a.map { |x| x.class.name }) }
a = [1, 1.5, "s", :sym, nil, true, false, (1..3), [9], { k: 1 }]
exp = a.map { |x| x.class.name }
w.send(a)
GC.compact
res = port.receive; w.value
raise "types #{res.inspect}" unless res == exp
puts "OK l56_diverse_array_types"
