# full mesh N=5 with move: payloads: fresh string per recipient, moved through inbox ports
# axes: move payload, 2 rounds, GC.compact at node 0 after rounds
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 5
R = 2
reg = Ractor::Port.new
done = Ractor::Port.new
nodes = N.times.map do |i|
  Ractor.new(i, reg, done, N, R) do |id, regp, dport, n, rounds|
    inbox = Ractor::Port.new
    regp.send([id, inbox])
    ports = Ractor.receive
    acc = 0
    rounds.times do |r|
      n.times do |j|
        next if j == id
        ports[j].send("#{id}:#{r}", move: true)
      end
      (n - 1).times do
        s = inbox.receive
        src, rr = s.split(":").map { |x| Integer(x) }
        acc += src * 10 + rr
      end
    end
    GC.compact if id == 0
    dport.send([id, acc])
    :fin
  end
end
port_by_id = Array.new(N)
N.times do
  id, p = reg.receive
  port_by_id[id] = p
end
nodes.each { |r| r.send(port_by_id) }
accs = Array.new(N)
N.times do
  id, a = done.receive
  accs[id] = a
end
N.times do |i|
  exp = (0...N).sum { |j| j == i ? 0 : (0...R).sum { |r| j * 10 + r } }
  raise "node #{i}" unless accs[i] == exp
end
GC.stress = false
nodes.each { |r| raise unless r.value == :fin }
puts "OK e02_mesh_move"
