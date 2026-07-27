# n-ary tree: root with 3 mids, each mid with 2 leaves (10 nodes), pull aggregation
# axes: mixed arity, per-level value tagging, GC.start in mids
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

def leaf2(v)
  Ractor.new(v) do |val|
    _tag, rp = Ractor.receive
    rp.send(val)
    :fin
  end
end

leaves = 6.times.map { |i| leaf2(i + 1) }
mids = 3.times.map do |m|
  Ractor.new(m, leaves[m * 2], leaves[m * 2 + 1]) do |mid, a, b|
    _tag, rp = Ractor.receive
    inbox = Ractor::Port.new
    a.send([:req, inbox])
    b.send([:req, inbox])
    GC.start
    rp.send(inbox.receive + inbox.receive + mid * 100)
    :fin
  end
end
root = Ractor.new(mids[0], mids[1], mids[2]) do |a, b, c|
  _tag, rp = Ractor.receive
  inbox = Ractor::Port.new
  [a, b, c].each { |x| x.send([:req, inbox]) }
  rp.send(inbox.receive + inbox.receive + inbox.receive)
  :fin
end
top = Ractor::Port.new
root.send([:req, top])
total = top.receive
exp = (1..6).sum + (0 + 100 + 200)
raise "#{total} != #{exp}" unless total == exp
GC.stress = false
([root] + mids + leaves).each { |r| raise unless r.value == :fin }
puts "OK e13_tree_tri_agg"
