# 双方向 ring を hash node で構成し make_shareable、前後走査
# axes: nodes=12 readers=4 loops=12 cyclic bidir
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
N = 12
nodes = Array.new(N) { |i| { v: i, nxt: nil, prev: nil } }
nodes.each_with_index do |node, i|
  node[:nxt] = nodes[(i + 1) % N]
  node[:prev] = nodes[(i - 1) % N]
end
GRAPH = Ractor.make_shareable(nodes[0])
STEPS = N * 12
EXP = (0...N).sum * 12
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

puts "OK i23_cyclic_graph_hash"
