# hash→array→Struct の混合 shareable graph、reader が集計
# axes: keys=16 per=8 readers=6 compacts=4 mixed
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
Rec = Struct.new(:id, :score)
KEYS = 16
PER = 8
h = {}
KEYS.times do |k|
  h[("g%02d" % k).freeze] = Array.new(PER) { |j| Rec.new(k * 100 + j, j + 1) }.freeze
end
GRAPH = Ractor.make_shareable(h.freeze)
EXP = KEYS * (1..PER).sum
rs = 6.times.map do |rid|
  Ractor.new(GRAPH, rid) do |g, id|
    acc = 0
    g.each_value { |arr| arr.each { |rec| acc += rec.score } }
    acc
  end
end
4.times { GC.compact }
rs.each { |ra| raise "mismatch" unless ra.value == EXP }

puts "OK i52_hash_of_arrays_of_structs"
