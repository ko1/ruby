# binary tree aggregation of Structs: leaves emit Stat structs, parents fold min/max/sum
# axes: Struct payload folded per level, frozen shareable seed table
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

Stat = Struct.new(:min, :max, :sum, :n)
SEEDS = Ractor.make_shareable([7, 3, 11, 5].freeze)

def fold(a, b)
  Stat.new([a.min, b.min].min, [a.max, b.max].max, a.sum + b.sum, a.n + b.n)
end

leaves = 4.times.map do |i|
  Ractor.new(i) do |id|
    _tag, rp = Ractor.receive
    v = SEEDS[id]
    rp.send(Stat.new(v, v, v, 1))
    :fin
  end
end
mids = 2.times.map do |m|
  Ractor.new(leaves[m * 2], leaves[m * 2 + 1]) do |a, b|
    _tag, rp = Ractor.receive
    inbox = Ractor::Port.new
    a.send([:req, inbox])
    b.send([:req, inbox])
    rp.send(fold(inbox.receive, inbox.receive))
    :fin
  end
end
root = Ractor.new(mids[0], mids[1]) do |a, b|
  _tag, rp = Ractor.receive
  inbox = Ractor::Port.new
  a.send([:req, inbox])
  b.send([:req, inbox])
  rp.send(fold(inbox.receive, inbox.receive))
  :fin
end
top = Ractor::Port.new
root.send([:req, top])
st = top.receive
raise "bad stat #{st}" unless st == Stat.new(3, 11, 26, 4)
GC.stress = false
([root] + mids + leaves).each { |r| raise unless r.value == :fin }
puts "OK e18_tree_struct_agg"
