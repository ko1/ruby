# 各 Ractor が shareable を量産(population トリガ)し main が global GC
# axes: many-ractor, make_shareable, global-trigger
Warning[:experimental] = false
N = ENV['S_STRESS'] ? 32 : 128
GC.stress = true if ENV['S_STRESS']
rs = Array.new(N) do |i|
  Ractor.new(i) { |id| 50.times.map { |k| Ractor.make_shareable("sh#{id}_#{k}".freeze) }.size }
end
GC.start
raise unless rs.map(&:value).all? { |v| v == 50 }
GC.start
puts "OK swarm_shareable_flood"
