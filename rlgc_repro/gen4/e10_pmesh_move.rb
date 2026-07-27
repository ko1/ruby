# partial mesh (ring+chord) N=6 with moved array payloads; receiver mutates then verifies
# axes: move payload through ports, husk check via fresh object per send
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 6
reg = Ractor::Port.new
done = Ractor::Port.new
nodes = N.times.map do |i|
  Ractor.new(i, reg, done, N) do |id, regp, dport, n|
    inbox = Ractor::Port.new
    regp.send([id, inbox])
    ports = Ractor.receive
    [(id + 1) % n, (id + 3) % n].each do |j|
      payload = [id, "from#{id}", [id * 2, id * 3]]
      ports[j].send(payload, move: true)
    end
    total = 0
    2.times do
      a = inbox.receive
      a << :seen
      raise "bad tag" unless a[1] == "from#{a[0]}"
      total += a[0] + a[2].sum
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
  ins = [(id - 1) % N, (id - 3) % N]
  exp = ins.sum { |j| j + j * 2 + j * 3 }
  raise "node #{id}" unless t == exp
end
GC.stress = false
nodes.each { |r| raise unless r.value == :fin }
puts "OK e10_pmesh_move"
