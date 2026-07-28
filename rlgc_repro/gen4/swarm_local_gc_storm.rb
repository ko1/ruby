# 全 Ractor が自分の local GC を並列に回す(lock-free local GC の同時多発)
# axes: many-ractor, parallel-local-GC
Warning[:experimental] = false
N = ENV['S_STRESS'] ? 16 : 48
GC.stress = true if ENV['S_STRESS']
rs = Array.new(N) do
  Ractor.new do
    buf = []
    20.times { |k| buf << "g#{k}" * 16; GC.start if k % 5 == 0; buf.shift if buf.size > 8 }
    buf.size
  end
end
GC.start
raise unless rs.map(&:value).all? { |v| v == 8 }
puts "OK swarm_local_gc_storm"
