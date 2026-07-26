# array/hash/Struct が交互に入れ子の深い shareable graph
# axes: depth=5 width=2 readers=6 compacts=4 mixed
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
MRec = Struct.new(:tag, :payload)
def build_mixed(d, w)
  return MRec.new(d, ("x" * (d + 1)).freeze) if d.zero?
  if d.even?
    Array.new(w) { build_mixed(d - 1, w) }.freeze
  else
    h = {}
    w.times { |i| h[("m%d" % i).freeze] = build_mixed(d - 1, w) }
    h.freeze
  end
end
def mixed_sum(o)
  case o
  when MRec then o.tag + o.payload.bytesize
  when Array then o.sum { |e| mixed_sum(e) }
  when Hash then o.sum { |_k, v| mixed_sum(v) }
  else 0
  end
end
GRAPH = Ractor.make_shareable(build_mixed(5, 2))
EXP = mixed_sum(GRAPH)
rs = 6.times.map do |rid|
  Ractor.new(GRAPH, rid) do |g, id|
    acc = 0
    5.times { acc += mixed_sum(g) }
    acc
  end
end
4.times { GC.compact }
rs.each { |ra| raise "mismatch" unless ra.value == EXP * 5 }

puts "OK i68_mixed_deep_nested"
