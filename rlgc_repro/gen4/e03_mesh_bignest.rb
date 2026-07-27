# full mesh N=3, single round, big nested graph payload (hash/array/string mix, copied)
# axes: deep copy of nested structure, checksum verified on both sides
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

def build_graph(seed, w, d)
  return [seed, "leaf#{seed}"] if d == 0
  w.times.map { |k| { id: seed + k, sub: build_graph(seed + k + 1, w, d - 1), tag: "n#{seed + k}" } }
end

def checksum(o)
  case o
  when Array then o.sum { |e| checksum(e) }
  when Hash then o.sum { |_k, v| checksum(v) }
  when Integer then o
  when String then o.length
  else 0
  end
end

N = 3
reg = Ractor::Port.new
done = Ractor::Port.new
nodes = N.times.map do |i|
  Ractor.new(i, reg, done, N) do |id, regp, dport, n|
    inbox = Ractor::Port.new
    regp.send([id, inbox])
    ports = Ractor.receive
    g = build_graph(id * 1000, 3, 3)
    my_ck = checksum(g)
    n.times { |j| ports[j].send([id, my_ck, g]) unless j == id }
    got = 0
    (n - 1).times do
      src, ck, graph = inbox.receive
      raise "ck mismatch from #{src}" unless checksum(graph) == ck
      got += 1
    end
    dport.send([id, got])
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
  _id, got = done.receive
  raise unless got == N - 1
end
GC.stress = false
GC.compact
nodes.each { |r| raise unless r.value == :fin }
puts "OK e03_mesh_bignest"
