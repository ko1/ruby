# 多数 producer が main の port へ copy 送信(fan-in)
# axes: many-ractor, port, copy
Warning[:experimental] = false
N = ENV['S_STRESS'] ? 32 : 128
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
rs = Array.new(N) { |i| Ractor.new(port, i) { |o, id| o.send([id, "payload#{id}", [id]*4]); :sent } }
got = N.times.map { port.receive }
raise unless got.size == N && got.all? { |(id, s, a)| s == "payload#{id}" && a.sum == id*4 }
rs.each(&:value)
GC.start
puts "OK swarm_fanin_port"
