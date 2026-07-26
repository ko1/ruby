# structurally diverse array payload; compact during window
# axes: copy/move, diverse array, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
w = Ractor.new(port) { |o| a = Ractor.receive; o.send([a.count { |x| x.is_a?(Symbol) }, a.count { |x| x.is_a?(String) }]) }
a = []
100.times { |i| a << :"sym#{i}"; a << "str#{i}" }
w.send(a)
GC.compact
syms, strs = port.receive; w.value
raise unless syms == 100 && strs == 100
puts "OK l59_diverse_array_symbols_strings"
