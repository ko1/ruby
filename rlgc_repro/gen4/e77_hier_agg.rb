# hierarchy: root -> 2 region heads -> 3 leaves each (9 nodes); pull aggregation up both levels
# axes: two-level request/response, region-tagged values, GC.start at heads
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

def hleaf(region, idx)
  Ractor.new(region, idx) do |reg, i|
    _tag, rp = Ractor.receive
    rp.send(reg * 100 + i)
    :fin
  end
end

leaves = []
heads = 2.times.map do |reg|
  ls = 3.times.map { |i| hleaf(reg, i) }
  leaves.concat(ls)
  Ractor.new(reg, ls[0], ls[1], ls[2]) do |r, a, b, c|
    _tag, rp = Ractor.receive
    inbox = Ractor::Port.new
    [a, b, c].each { |x| x.send([:req, inbox]) }
    GC.start
    s = inbox.receive + inbox.receive + inbox.receive
    rp.send([r, s])
    :fin
  end
end
root = Ractor.new(heads[0], heads[1]) do |h0, h1|
  _tag, rp = Ractor.receive
  inbox = Ractor::Port.new
  h0.send([:req, inbox])
  h1.send([:req, inbox])
  parts = {}
  2.times do
    r, s = inbox.receive
    parts[r] = s
  end
  rp.send(parts)
  :fin
end
top = Ractor::Port.new
root.send([:req, top])
parts = top.receive
2.times do |r|
  exp = (0...3).sum { |i| r * 100 + i }
  raise "region #{r}: #{parts[r]}" unless parts[r] == exp
end
GC.stress = false
([root] + heads + leaves).each { |r| raise unless r.value == :fin }
puts "OK e77_hier_agg"
