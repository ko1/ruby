# 混合: 常駐 worker 群 + churn + shareable + main の GC/compact
# axes: many-ractor, mixed-app
Warning[:experimental] = false
N = ENV['S_STRESS'] ? 24 : 80
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
workers = Array.new(N) do |i|
  Ractor.new(port, i) do |o, id|
    tbl = Ractor.make_shareable({ id: id, name: "w#{id}".freeze }.freeze)
    o.send(tbl[:id])
    Ractor.receive
    tbl[:name].size
  end
end
ids = N.times.map { port.receive }.sort
raise unless ids == (0...N).to_a
GC.compact
churn = Array.new(N/4) { |i| Ractor.new(i) { |x| Array.new(16) { +"t#{x}" }.size } }
raise unless churn.map(&:value).all? { |v| v == 16 }
GC.start
workers.each { |w| w.send(:fin) }
raise unless workers.map(&:value).all? { |v| v >= 2 }
puts "OK swarm_mixed_app"
