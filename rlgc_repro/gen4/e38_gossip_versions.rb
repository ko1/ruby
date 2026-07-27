# gossip version vectors on ring N=5: nodes merge max version per key over N rounds
# axes: Struct entries, hash merge semantics, GC.compact at node 2 at the end
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

Ver = Struct.new(:node, :ver)

N = 5
R = N
reg = Ractor::Port.new
done = Ractor::Port.new
nodes = N.times.map do |i|
  Ractor.new(i, reg, done, R) do |id, regp, dport, rounds|
    inbox = Ractor::Port.new
    regp.send([id, inbox])
    left, right = Ractor.receive
    vv = { id => Ver.new(id, id * 3 + 1) }
    rounds.times do
      snap = vv.values.map { |v| Ver.new(v.node, v.ver) }
      left.send(snap)
      right.send(snap)
      2.times do
        inbox.receive.each do |v|
          cur = vv[v.node]
          vv[v.node] = v if cur.nil? || v.ver > cur.ver
        end
      end
    end
    GC.compact if id == 2
    dport.send([id, vv.keys.sort.map { |k| [k, vv[k].ver] }])
    :fin
  end
end
port_by_id = Array.new(N)
N.times do
  id, p = reg.receive
  port_by_id[id] = p
end
N.times { |i| nodes[i].send([port_by_id[(i - 1) % N], port_by_id[(i + 1) % N]]) }
exp = (0...N).map { |k| [k, k * 3 + 1] }
N.times do
  id, vec = done.receive
  raise "node #{id}: #{vec}" unless vec == exp
end
GC.stress = false
nodes.each { |r| raise unless r.value == :fin }
puts "OK e38_gossip_versions"
