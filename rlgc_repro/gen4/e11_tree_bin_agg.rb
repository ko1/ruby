# binary tree depth 2 (7 nodes): pull-based aggregation, leaves reply to parent's port
# axes: bottom-up build (children passed as args), request/response over ports
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

def leaf(v)
  Ractor.new(v) do |val|
    _tag, rp = Ractor.receive
    rp.send(val)
    :fin
  end
end

def internal(v, l, r)
  Ractor.new(v, l, r) do |val, lc, rc|
    _tag, rp = Ractor.receive
    inbox = Ractor::Port.new
    lc.send([:req, inbox])
    rc.send([:req, inbox])
    s = inbox.receive + inbox.receive + val
    rp.send(s)
    :fin
  end
end

leaves = 4.times.map { |i| leaf(10 + i) }
mids = [internal(100, leaves[0], leaves[1]), internal(200, leaves[2], leaves[3])]
root = internal(1000, mids[0], mids[1])
top = Ractor::Port.new
root.send([:req, top])
total = top.receive
exp = 1000 + 100 + 200 + (10 + 11 + 12 + 13)
raise "#{total} != #{exp}" unless total == exp
GC.stress = false
GC.compact
([root] + mids + leaves).each { |r| raise unless r.value == :fin }
puts "OK e11_tree_bin_agg"
