# full mesh N=4: every node sends to every other over 3 rounds via per-node inbox ports
# axes: copy payload, scattered GC.start at even nodes, port-based wiring
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 4
R = 3
reg = Ractor::Port.new
done = Ractor::Port.new
nodes = N.times.map do |i|
  Ractor.new(i, reg, done, N, R) do |id, regp, dport, n, rounds|
    inbox = Ractor::Port.new
    regp.send([id, inbox])
    ports = Ractor.receive
    total = 0
    rounds.times do |r|
      n.times { |j| ports[j].send([id, r, id * 100 + r]) unless j == id }
      (n - 1).times do
        _src, _rr, v = inbox.receive
        total += v
      end
      GC.start if r == 1 && id.even?
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
sums = Array.new(N)
N.times do
  id, t = done.receive
  sums[id] = t
end
N.times do |i|
  exp = (0...N).sum { |j| j == i ? 0 : (0...R).sum { |r| j * 100 + r } }
  raise "node #{i}: #{sums[i]} != #{exp}" unless sums[i] == exp
end
GC.stress = false
nodes.each { |r| raise unless r.value == :fin }
puts "OK e01_mesh_rounds"
