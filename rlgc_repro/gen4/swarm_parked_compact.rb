# 多数の parked Ractor の下で main が GC.compact(参照更新の page_index 経路)
# axes: many-ractor, parked, compact
Warning[:experimental] = false
N = ENV['S_STRESS'] ? 48 : 192
GC.stress = true if ENV['S_STRESS']
rs = Array.new(N) { Ractor.new { h = { a: +"s", b: [1,2,3] }; Ractor.receive; h[:b].sum } }
2.times { GC.compact }
rs.each { |r| r.send(:go) }
raise unless rs.map(&:value).all? { |v| v == 6 }
puts "OK swarm_parked_compact"
