# hierarchy with move: leaves move record batches up, heads merge and move to root
# axes: move at both levels, merged array ordering normalized, 2 regions x 2 leaves
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

def mleaf(region, idx)
  Ractor.new(region, idx) do |reg, i|
    _tag, rp = Ractor.receive
    batch = 3.times.map { |k| { reg: reg, leaf: i, seq: k, blob: "b#{reg}#{i}#{k}" } }
    rp.send(batch, move: true)
    :fin
  end
end

leaves = []
heads = 2.times.map do |reg|
  ls = 2.times.map { |i| mleaf(reg, i) }
  leaves.concat(ls)
  Ractor.new(reg, ls[0], ls[1]) do |r, a, b|
    _tag, rp = Ractor.receive
    inbox = Ractor::Port.new
    a.send([:req, inbox])
    b.send([:req, inbox])
    merged = inbox.receive
    merged.concat(inbox.receive)
    merged.each { |rec| rec[:via] = r }
    rp.send(merged, move: true)
    :fin
  end
end
root = Ractor.new(heads[0], heads[1]) do |h0, h1|
  _tag, rp = Ractor.receive
  inbox = Ractor::Port.new
  h0.send([:req, inbox])
  h1.send([:req, inbox])
  all = inbox.receive
  all.concat(inbox.receive)
  rp.send(all, move: true)
  :fin
end
top = Ractor::Port.new
root.send([:req, top])
all = top.receive
raise "size #{all.size}" unless all.size == 12
all.each do |rec|
  raise "via" unless rec[:via] == rec[:reg]
  raise "blob" unless rec[:blob] == "b#{rec[:reg]}#{rec[:leaf]}#{rec[:seq]}"
end
sigs = all.map { |rec| [rec[:reg], rec[:leaf], rec[:seq]] }.sort
exp = []
2.times { |r| 2.times { |l| 3.times { |k| exp << [r, l, k] } } }
raise unless sigs == exp
GC.stress = false
([root] + heads + leaves).each { |x| raise unless x.value == :fin }
puts "OK e79_hier_move"
