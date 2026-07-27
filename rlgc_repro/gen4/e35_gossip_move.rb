# gossip on ring N=4 with moved payloads: fresh array copy moved to each neighbor per round
# axes: move payload in gossip rounds, union of ivar'd items by key
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

class Item
  attr_reader :key, :val
  def initialize(k, v)
    @key = k
    @val = v
  end
end

N = 4
R = N
reg = Ractor::Port.new
done = Ractor::Port.new
nodes = N.times.map do |i|
  Ractor.new(i, reg, done, R) do |id, regp, dport, rounds|
    inbox = Ractor::Port.new
    regp.send([id, inbox])
    left, right = Ractor.receive
    known = { id => Item.new(id, id * 11) }
    rounds.times do
      left.send(known.values.map { |it| Item.new(it.key, it.val) }, move: true)
      right.send(known.values.map { |it| Item.new(it.key, it.val) }, move: true)
      2.times do
        inbox.receive.each { |it| known[it.key] ||= it }
      end
    end
    sig = known.keys.sort.map { |k| "#{k}:#{known[k].val}" }.join(",")
    dport.send([id, sig])
    :fin
  end
end
port_by_id = Array.new(N)
N.times do
  id, p = reg.receive
  port_by_id[id] = p
end
N.times { |i| nodes[i].send([port_by_id[(i - 1) % N], port_by_id[(i + 1) % N]]) }
exp = (0...N).map { |k| "#{k}:#{k * 11}" }.join(",")
N.times do
  id, sig = done.receive
  raise "node #{id}: #{sig}" unless sig == exp
end
GC.stress = false
nodes.each { |r| raise unless r.value == :fin }
puts "OK e35_gossip_move"
