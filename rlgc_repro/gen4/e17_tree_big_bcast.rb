# binary tree broadcast of a big nested graph down to 4 leaves; leaves checksum and ack
# axes: big copied payload replicated at each level, GC.compact at one leaf
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

def mkgraph(seed)
  4.times.map { |i| { k: seed + i, arr: (0..20).to_a.map { |x| x * (seed + i) }, s: "seg#{seed + i}" * 3 } }
end

def gsum(g)
  g.sum { |h| h[:k] + h[:arr].sum + h[:s].length }
end

ack = Ractor::Port.new
leaves = 4.times.map do |i|
  Ractor.new(i, ack) do |id, ackp|
    g = Ractor.receive
    GC.compact if id == 3
    ackp.send([id, gsum(g)])
    :fin
  end
end
mids = 2.times.map do |m|
  Ractor.new(leaves[m * 2], leaves[m * 2 + 1]) do |a, b|
    g = Ractor.receive
    a.send(g)
    b.send(g)
    :fin
  end
end
root = Ractor.new(mids[0], mids[1]) do |a, b|
  g = Ractor.receive
  a.send(g)
  b.send(g)
  :fin
end
g = mkgraph(5)
exp = gsum(g)
root.send(g)
4.times do
  _id, ck = ack.receive
  raise "ck #{ck} != #{exp}" unless ck == exp
end
GC.stress = false
([root] + mids + leaves).each { |r| raise unless r.value == :fin }
puts "OK e17_tree_big_bcast"
