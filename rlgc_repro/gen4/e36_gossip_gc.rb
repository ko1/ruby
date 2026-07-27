# gossip on ring N=6 with GC.start every round at even nodes and GC.compact once at node 1
# axes: GC/compact interleaved with gossip unions
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 6
R = N
reg = Ractor::Port.new
done = Ractor::Port.new
nodes = N.times.map do |i|
  Ractor.new(i, reg, done, R) do |id, regp, dport, rounds|
    inbox = Ractor::Port.new
    regp.send([id, inbox])
    left, right = Ractor.receive
    known = [id]
    rounds.times do |r|
      snap = known.dup
      left.send(snap)
      right.send(snap)
      GC.start if id.even?
      GC.compact if id == 1 && r == rounds / 2
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
  raise "node #{id}" unless known == all
end
GC.stress = false
nodes.each { |r| raise unless r.value == :fin }
puts "OK e36_gossip_gc"
