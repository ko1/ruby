# 深くネストした frozen hash を reader が deep_sum、compact 反復
# axes: depth=3 width=4 readers=4 compacts=12
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def deep_sum(o)
  case o
  when Integer then o
  when String then o.bytesize
  when Array then o.sum { |e| deep_sum(e) }
  when Hash then o.sum { |_k, v| deep_sum(v) }
  else 0
  end
end
def build_hash(d, w)
  return d if d.zero?
  h = {}
  w.times { |i| h[("k%02d" % i).freeze] = build_hash(d - 1, w) }
  h.freeze
end
GRAPH = Ractor.make_shareable(build_hash(3, 4))
EXP = deep_sum(GRAPH)
rs = 4.times.map do |rid|
  Ractor.new(GRAPH, rid) do |g, id|
    acc = 0
    6.times { acc += deep_sum(g) }
    acc
  end
end
12.times { GC.compact }
rs.each { |ra| raise "mismatch" unless ra.value == EXP * 6 }

puts "OK i12_nested_frozen_hash"
