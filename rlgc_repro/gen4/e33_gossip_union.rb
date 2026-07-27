# gossip on ring N=6: each round push known-id set to both neighbors, union in; R=N rounds
# axes: fixed deterministic rounds, copy payload (sorted arrays), full-knowledge assertion
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 6
R = N
reg = Ractor::Port.new
done = Ractor::Port.new
nodes = N.times.map do |i|
  Ractor.new(i, reg, done, N, R) do |id, regp, dport, n, rounds|
    inbox = Ractor::Port.new
    regp.send([id, inbox])
    left, right = Ractor.receive
    known = [id]
    rounds.times do
      snap = known.sort
      left.send(snap)
      right.send(snap)
      2.times { known |= inbox.receive }
    end
    dport.send([id, known.sort])
    :fin
  end
end
port_by_id = Array.new(N)
N.times do
  id, p = reg.receive
  port_by_id[id] = p
end
N.times { |i| nodes[i].send([port_by_id[(i - 1) % N], port_by_id[(i + 1) % N]]) }
all = (0...N).to_a
N.times do
  id, known = done.receive
  raise "node #{id}: #{known}" unless known == all
end
GC.stress = false
nodes.each { |r| raise unless r.value == :fin }
puts "OK e33_gossip_union"
