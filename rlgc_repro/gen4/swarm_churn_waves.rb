# 生成→join を波状に繰り返す(zombie_objspaces の絶え間ない登録/absorb)
# axes: many-ractor, lifecycle-churn, zombie
Warning[:experimental] = false
W = ENV['S_STRESS'] ? 3 : 8
N = ENV['S_STRESS'] ? 16 : 64
GC.stress = true if ENV['S_STRESS']
W.times do |w|
  rs = Array.new(N) { |i| Ractor.new(w, i) { |a, b| Array.new(32) { +"c#{a}_#{b}" }.size } }
  raise unless rs.map(&:value).all? { |v| v == 32 }
  GC.start if w.even?
end
puts "OK swarm_churn_waves"
