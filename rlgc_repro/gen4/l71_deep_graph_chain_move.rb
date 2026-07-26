# deep/wide object graph moved; compact during window
# axes: move, deep graph, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
w = Ractor.new(port) do |o|
  node = Ractor.receive
  d = 0
  while node[:next]; d += 1; node = node[:next]; end
  o.send(d)
end
head = { val: 0, next: nil }
600.times { |i| head = { val: i + 1, next: head } }
w.send(head, move: true)
GC.compact
d = port.receive; w.value
raise "chain #{d}" unless d == 600
puts "OK l71_deep_graph_chain_move"
