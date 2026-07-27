# binary tree aggregation with GC.start+GC.compact at each leaf before reply
# axes: GC at leaves under request load, 2 aggregation waves
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

WAVES = 2
leaves = 4.times.map do |i|
  Ractor.new(i, WAVES) do |id, waves|
    waves.times do
      _tag, rp = Ractor.receive
      GC.start
      GC.compact if id == 0
      rp.send(id + 1)
    end
    :fin
  end
end
mids = 2.times.map do |m|
  Ractor.new(leaves[m * 2], leaves[m * 2 + 1], WAVES) do |a, b, waves|
    waves.times do
      _tag, rp = Ractor.receive
      inbox = Ractor::Port.new
      a.send([:req, inbox])
      b.send([:req, inbox])
      rp.send(inbox.receive + inbox.receive)
    end
    :fin
  end
end
root = Ractor.new(mids[0], mids[1], WAVES) do |a, b, waves|
  waves.times do
    _tag, rp = Ractor.receive
    inbox = Ractor::Port.new
    a.send([:req, inbox])
    b.send([:req, inbox])
    rp.send(inbox.receive + inbox.receive)
  end
  :fin
end
top = Ractor::Port.new
WAVES.times do
  root.send([:req, top])
  raise unless top.receive == 1 + 2 + 3 + 4
end
GC.stress = false
([root] + mids + leaves).each { |r| raise unless r.value == :fin }
puts "OK e14_tree_gc_leaves"
