# full mesh N=4 exchanging Struct + ivar'd object payloads (copied)
# axes: Struct payload, plain object with ivars, 2 rounds
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

Msg = Struct.new(:src, :round, :body)
class Body
  attr_reader :val, :note
  def initialize(v, n)
    @val = v
    @note = n
  end
end

N = 4
R = 2
reg = Ractor::Port.new
done = Ractor::Port.new
nodes = N.times.map do |i|
  Ractor.new(i, reg, done, N, R) do |id, regp, dport, n, rounds|
    inbox = Ractor::Port.new
    regp.send([id, inbox])
    ports = Ractor.receive
    total = 0
    rounds.times do |r|
      n.times do |j|
        next if j == id
        ports[j].send(Msg.new(id, r, Body.new(id * 7 + r, "b#{id}")))
      end
      (n - 1).times do
        m = inbox.receive
        raise "bad note" unless m.body.note == "b#{m.src}"
        total += m.body.val
      end
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
  exp = (0...N).sum { |j| j == id ? 0 : (0...R).sum { |r| j * 7 + r } }
  raise "node #{id}" unless t == exp
end
GC.stress = false
nodes.each { |r| raise unless r.value == :fin }
puts "OK e05_mesh_struct"
