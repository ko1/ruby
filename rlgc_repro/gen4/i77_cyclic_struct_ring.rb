# Struct ring + Data メタで cyclic shareable、周回で v+order 集計
# axes: ring=6 readers=8 loops=20 struct data cyclic
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
Cell = Struct.new(:v, :nxt, :meta)
Meta = Data.define(:label, :order)
N = 6
cells = Array.new(N) { |i| Cell.new(i, nil, Meta.new(label: ("c%02d" % i).freeze, order: i)) }
cells.each_with_index { |c, i| c.nxt = cells[(i + 1) % N] }
RING = Ractor.make_shareable(cells[0])
STEPS = N * 20
EXP = (0...N).sum { |i| i + i } * 20
rs = 8.times.map do |rid|
  Ractor.new(RING, rid) do |start, id|
    cur = start
    acc = 0
    STEPS.times { acc += cur.v + cur.meta.order; cur = cur.nxt }
    acc
  end
end
6.times { GC.compact }
rs.each { |ra| raise "mismatch" unless ra.value == EXP }

puts "OK i77_cyclic_struct_ring"
