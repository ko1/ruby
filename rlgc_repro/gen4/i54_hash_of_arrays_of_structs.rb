# hash→array→Struct の混合 shareable graph、reader が集計
# axes: keys=5 per=25 readers=4 compacts=7 mixed
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
Rec = Struct.new(:id, :score)
KEYS = 5
PER = 25
h = {}
KEYS.times do |k|
  h[("g%02d" % k).freeze] = Array.new(PER) { |j| Rec.new(k * 100 + j, j + 1) }.freeze
end
GRAPH = Ractor.make_shareable(h.freeze)
EXP = KEYS * (1..PER).sum
rs = 4.times.map do |rid|
  Ractor.new(GRAPH, rid) do |g, id|
    acc = 0
    g.each_value { |arr| arr.each { |rec| acc += rec.score } }
    acc
  end
end
7.times { GC.compact }
rs.each { |ra| raise "mismatch" unless ra.value == EXP }

puts "OK i54_hash_of_arrays_of_structs"
