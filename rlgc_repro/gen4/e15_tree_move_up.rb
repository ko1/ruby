# binary tree: leaves move arrays up, mids concat and move up, root checks merged order-insensitively
# axes: move on every hop upward, mutable payload grown at each level
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

leaves = 4.times.map do |i|
  Ractor.new(i) do |id|
    _tag, rp = Ractor.receive
    rp.send([[id, "leaf#{id}"]], move: true)
    :fin
  end
end
mids = 2.times.map do |m|
  Ractor.new(m, leaves[m * 2], leaves[m * 2 + 1]) do |mid, a, b|
    _tag, rp = Ractor.receive
    inbox = Ractor::Port.new
    a.send([:req, inbox])
    b.send([:req, inbox])
    merged = inbox.receive
    merged.concat(inbox.receive)
    merged << [100 + mid, "mid#{mid}"]
    rp.send(merged, move: true)
    :fin
  end
end
root = Ractor.new(mids[0], mids[1]) do |a, b|
  _tag, rp = Ractor.receive
  inbox = Ractor::Port.new
  a.send([:req, inbox])
  b.send([:req, inbox])
  all = inbox.receive
  all.concat(inbox.receive)
  rp.send(all, move: true)
  :fin
end
top = Ractor::Port.new
root.send([:req, top])
all = top.receive
ids = all.map(&:first).sort
raise "ids #{ids}" unless ids == [0, 1, 2, 3, 100, 101]
all.each do |id, tag|
  raise unless tag == (id >= 100 ? "mid#{id - 100}" : "leaf#{id}")
end
GC.stress = false
GC.compact
([root] + mids + leaves).each { |r| raise unless r.value == :fin }
puts "OK e15_tree_move_up"
