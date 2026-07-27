# gossip via symmetric round-robin pairing p(i,r)=(r-i) mod N over N=6, N rounds; self-pair sits out
# axes: pairwise deterministic exchange schedule, copy payload
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
    ports = Ractor.receive
    known = [id]
    rounds.times do |r|
      partner = (r - id) % n
      next if partner == id
      ports[partner].send(known.dup)
      known |= inbox.receive
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
nodes.each { |r| r.send(port_by_id) }
all = (0...N).to_a
N.times do
  id, known = done.receive
  raise "node #{id}: #{known}" unless known == all
end
GC.stress = false
nodes.each { |r| raise unless r.value == :fin }
puts "OK e37_gossip_pairs"
