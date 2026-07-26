# 双方向 ring を hash node で構成し make_shareable、前後走査
# axes: nodes=6 readers=4 loops=18 cyclic bidir
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
N = 6
nodes = Array.new(N) { |i| { v: i, nxt: nil, prev: nil } }
nodes.each_with_index do |node, i|
  node[:nxt] = nodes[(i + 1) % N]
  node[:prev] = nodes[(i - 1) % N]
end
GRAPH = Ractor.make_shareable(nodes[0])
STEPS = N * 18
EXP = (0...N).sum * 18
rs = 4.times.map do |rid|
  Ractor.new(GRAPH, rid) do |start, id|
    cur = start
    acc = 0
    STEPS.times { acc += cur[:v]; cur = cur[:nxt] }
    # walk back too to exercise both edges
    STEPS.times { cur = cur[:prev] }
    acc
  end
end
6.times { GC.compact }
rs.each { |ra| raise "mismatch" unless ra.value == EXP }

puts "OK i19_cyclic_graph_hash"
