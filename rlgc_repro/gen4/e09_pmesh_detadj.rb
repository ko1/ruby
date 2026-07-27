# partial mesh: adjacency from a deterministic formula, expected in-counts precomputed in main
# axes: irregular directed topology N=7, copy payload, per-node expected counts passed as arg
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 7
def edge?(i, j)
  i != j && ((i * 7 + j * 13) % 5) < 2
end

in_count = Array.new(N) { |j| (0...N).count { |i| edge?(i, j) } }
reg = Ractor::Port.new
done = Ractor::Port.new
nodes = N.times.map do |i|
  Ractor.new(i, reg, done, N, in_count[i]) do |id, regp, dport, n, expect|
    inbox = Ractor::Port.new
    regp.send([id, inbox])
    ports = Ractor.receive
    n.times { |j| ports[j].send(id) if edge?(id, j) }
    srcs = []
    expect.times { srcs << inbox.receive }
    dport.send([id, srcs.sort])
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
  id, srcs = done.receive
  exp = (0...N).select { |i| edge?(i, id) }.sort
  raise "node #{id}: #{srcs} != #{exp}" unless srcs == exp
end
GC.stress = false
nodes.each { |r| raise unless r.value == :fin }
puts "OK e09_pmesh_detadj"
