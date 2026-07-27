# partial mesh: ring + chord (i -> i+1, i -> i+2) over N=8, 2 rounds, in-degree 2 per node
# axes: directed partial mesh, copy payload, GC.start at odd nodes
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 8
R = 2
reg = Ractor::Port.new
done = Ractor::Port.new
nodes = N.times.map do |i|
  Ractor.new(i, reg, done, N, R) do |id, regp, dport, n, rounds|
    inbox = Ractor::Port.new
    regp.send([id, inbox])
    ports = Ractor.receive
    outs = [(id + 1) % n, (id + 2) % n]
    total = 0
    rounds.times do |r|
      outs.each { |j| ports[j].send(id * 10 + r) }
      GC.start if id.odd?
      2.times { total += inbox.receive }
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
  ins = [(id - 1) % N, (id - 2) % N]
  exp = ins.sum { |j| (0...R).sum { |r| j * 10 + r } }
  raise "node #{id}: #{t} != #{exp}" unless t == exp
end
GC.stress = false
nodes.each { |r| raise unless r.value == :fin }
puts "OK e07_pmesh_chords"
