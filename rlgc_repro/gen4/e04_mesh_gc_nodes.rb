# full mesh N=6, 2 rounds, GC.start at every node each round + GC.compact at node 0 mid-flight
# axes: heavy per-node GC while mesh traffic is in flight
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 6
R = 2
reg = Ractor::Port.new
done = Ractor::Port.new
nodes = N.times.map do |i|
  Ractor.new(i, reg, done, N, R) do |id, regp, dport, n, rounds|
    inbox = Ractor::Port.new
    regp.send([id, inbox])
    ports = Ractor.receive
    total = 0
    rounds.times do |r|
      n.times { |j| ports[j].send([id, [r, id]]) unless j == id }
      GC.start
      GC.compact if id == 0 && r == 0
      (n - 1).times do
        src, pair = inbox.receive
        total += src + pair[0] * 1000 + pair[1]
      end
    end
    dport.send([id, total])
    :fin
  end
end
port_by_id = Array.new(N)
N.times do
  id, p = reg.receive
  port_by_id[id] = p
end
nodes.each { |r| r.send(port_by_id) }
N.times do
  id, t = done.receive
  exp = (0...N).sum { |j| j == id ? 0 : (0...R).sum { |r| j + r * 1000 + j } }
  raise "node #{id}: #{t} != #{exp}" unless t == exp
end
GC.stress = false
nodes.each { |r| raise unless r.value == :fin }
puts "OK e04_mesh_gc_nodes"
