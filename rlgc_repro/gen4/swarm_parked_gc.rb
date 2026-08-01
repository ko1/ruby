# 多数の parked Ractor の下で main が global GC を繰り返す
# axes: many-ractor, parked, GC.start
Warning[:experimental] = false
N = ENV['S_STRESS'] ? 64 : 256
GC.stress = true if ENV['S_STRESS']
rs = Array.new(N) { Ractor.new { a = Array.new(64) { +"x" }; Ractor.receive; a.size } }
3.times { GC.start }
rs.each { |r| r.send(:go) }
raise unless rs.map(&:value).all? { |v| v == 64 }
puts "OK swarm_parked_gc"
