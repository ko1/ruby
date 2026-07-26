# 深くネストした frozen array を全 reader が deep_sum、compact 反復
# axes: depth=3 width=4 readers=4 compacts=6
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
def build_arr(d, w)
  return d if d.zero?
  Array.new(w) { |i| build_arr(d - 1, w) }.freeze
end
GRAPH = Ractor.make_shareable(build_arr(3, 4))
EXP = deep_sum(GRAPH)
rs = 4.times.map do |rid|
  Ractor.new(GRAPH, rid) do |g, id|
    acc = 0
    6.times { acc += deep_sum(g) }
    [id, acc]
  end
end
6.times { GC.compact }
rs.each { |ra| id, acc = ra.value; raise "mismatch" unless acc == EXP * 6 }

puts "OK i03_nested_frozen_array"
