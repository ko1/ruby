# 自己参照 array で ring を作り make_shareable、reader が周回走査
# axes: ring=12 readers=5 loops=15 cyclic
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
N = 12
ring = Array.new(N) { |i| [i, nil] }
ring.each_with_index { |node, i| node[1] = ring[(i + 1) % N] }
RING = Ractor.make_shareable(ring[0])
STEPS = N * 15
EXP = (0...N).sum * 15
rs = 5.times.map do |rid|
  Ractor.new(RING, rid) do |start, id|
    cur = start
    acc = 0
    STEPS.times { acc += cur[0]; cur = cur[1] }
    acc
  end
end
7.times { GC.compact }
rs.each { |ra| raise "mismatch" unless ra.value == EXP }

puts "OK i18_cyclic_ring_array"
