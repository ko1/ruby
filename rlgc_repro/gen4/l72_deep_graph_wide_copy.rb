# deep/wide object graph copied; compact during window
# axes: copy, deep graph, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
w = Ractor.new(port) { |o| g = Ractor.receive; o.send(g.values.map(&:size).sum) }
g = {}
100.times { |i| g["node#{i}"] = Array.new(i % 10 + 1) { |j| { edge: j } } }
exp = g.values.map(&:size).sum
w.send(g, move: false)
GC.compact
res = port.receive; w.value
raise "wide #{res}!=#{exp}" unless res == exp
puts "OK l72_deep_graph_wide_copy"
