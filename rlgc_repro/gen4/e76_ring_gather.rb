# gathering ring N=8: token makes one lap collecting [id, id*id+7] from each node in order
# axes: ordered path accumulation, copy token, GC.start at node 4
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 8
done = Ractor::Port.new
nodes = N.times.map do |i|
  Ractor.new(i, done, N) do |id, dport, n|
    nxt = Ractor.receive
    loop do
      m = Ractor.receive
      break if m == :stop
      GC.start if id == 4
      m << [id, id * id + 7]
      if m.length - 1 >= n
        dport.send(m)
      else
        nxt.send(m)
      end
    end
    :fin
  end
end
N.times { |i| nodes[i].send(nodes[(i + 1) % N]) }
nodes[0].send([[:seed, 99]])
out = done.receive
raise unless out[0] == [:seed, 99]
exp = (0...N).map { |i| [i, i * i + 7] }
raise "gathered #{out[1..]}" unless out[1..] == exp
nodes.each { |r| r.send(:stop) }
GC.stress = false
nodes.each { |r| raise unless r.value == :fin }
puts "OK e76_ring_gather"
