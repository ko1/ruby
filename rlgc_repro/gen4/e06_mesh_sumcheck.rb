# full mesh N=12 (max width), single round, tiny int payloads, global sum verified in main
# axes: wide mesh, 132 messages, GC.compact in main while mesh runs
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 12
reg = Ractor::Port.new
done = Ractor::Port.new
nodes = N.times.map do |i|
  Ractor.new(i, reg, done, N) do |id, regp, dport, n|
    inbox = Ractor::Port.new
    regp.send([id, inbox])
    ports = Ractor.receive
    n.times { |j| ports[j].send(id + 1) unless j == id }
    s = 0
    (n - 1).times { s += inbox.receive }
    dport.send(s)
    :fin
  end
end
port_by_id = Array.new(N)
N.times do
  id, p = reg.receive
  port_by_id[id] = p
end
nodes.each { |r| r.send(port_by_id) }
GC.compact
grand = 0
N.times { grand += done.receive }
per_node_all = (1..N).sum
exp = (0...N).sum { |i| per_node_all - (i + 1) }
raise "grand #{grand} != #{exp}" unless grand == exp
GC.stress = false
nodes.each { |r| raise unless r.value == :fin }
puts "OK e06_mesh_sumcheck"
