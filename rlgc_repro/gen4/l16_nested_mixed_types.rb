# nested mixed structure copied; compact during window
# axes: copy, nested mixed, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
w = Ractor.new(port) { |o| a = Ractor.receive; o.send([a[0], a[1], a[2].round(3), a[3], a[4], a[5].length]) }
a = [42, 10_000_000_000_000_000_000_000, 3.14159, :sym, nil, [1, 2, 3]]
exp = [42, 10_000_000_000_000_000_000_000, 3.142, :sym, nil, 3]
w.send(a)
GC.compact
res = port.receive; w.value
raise "mix #{res.inspect}" unless res == exp
puts "OK l16_nested_mixed_types"
