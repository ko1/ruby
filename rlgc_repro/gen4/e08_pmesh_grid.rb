# partial mesh: 3x3 grid, each node exchanges once with orthogonal neighbors
# axes: undirected grid topology, degree varies 2-4, array payload
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

W = 3
N = W * W
def grid_neighbors(id, w)
  x = id % w
  y = id / w
  ns = []
  ns << id - 1 if x > 0
  ns << id + 1 if x < w - 1
  ns << id - w if y > 0
  ns << id + w if y < w - 1
  ns
end

reg = Ractor::Port.new
done = Ractor::Port.new
nodes = N.times.map do |i|
  Ractor.new(i, reg, done, W) do |id, regp, dport, w|
    inbox = Ractor::Port.new
    regp.send([id, inbox])
    ports = Ractor.receive
    ns = grid_neighbors(id, w)
    ns.each { |j| ports[j].send([id, id % w, id / w]) }
    seen = []
    ns.size.times do
      src, x, y = inbox.receive
      raise "bad coords" unless src == y * w + x
      seen << src
    end
    dport.send([id, seen.sort])
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
  id, seen = done.receive
  raise "node #{id}" unless seen == grid_neighbors(id, W).sort
end
GC.stress = false
GC.compact
nodes.each { |r| raise unless r.value == :fin }
puts "OK e08_pmesh_grid"
