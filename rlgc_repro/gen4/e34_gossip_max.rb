# gossip max on ring N=5: seeded values propagate, every node must converge to the global max
# axes: scalar gossip, R=N rounds, GC.start at node with the max seed
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 5
R = N
seeds = N.times.map { |i| (i * 37) % 23 }
reg = Ractor::Port.new
done = Ractor::Port.new
nodes = N.times.map do |i|
  Ractor.new(i, seeds[i], reg, done, R) do |id, seed, regp, dport, rounds|
    inbox = Ractor::Port.new
    regp.send([id, inbox])
    left, right = Ractor.receive
    best = seed
    rounds.times do
      left.send(best)
      right.send(best)
      GC.start if seed >= 20
      2.times do
        v = inbox.receive
        best = v if v > best
      end
    end
    dport.send([id, best])
    :fin
  end
end
port_by_id = Array.new(N)
N.times do
  id, p = reg.receive
  port_by_id[id] = p
end
N.times { |i| nodes[i].send([port_by_id[(i - 1) % N], port_by_id[(i + 1) % N]]) }
gmax = seeds.max
N.times do
  id, best = done.receive
  raise "node #{id}: #{best} != #{gmax}" unless best == gmax
end
GC.stress = false
nodes.each { |r| raise unless r.value == :fin }
puts "OK e34_gossip_max"
