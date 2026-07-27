# hierarchy broadcast: root pushes config to 2 regions x 4 leaves; heads aggregate leaf acks
# axes: broadcast down + aggregated ack up, copy payload, counts checked at each level
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

def bleaf(region, idx)
  Ractor.new(region, idx) do |reg, i|
    cfg, ackp = Ractor.receive
    ackp.send([reg, i, cfg[:epoch] + reg * 10 + i])
    :fin
  end
end

leaves = []
heads = 2.times.map do |reg|
  ls = 4.times.map { |i| bleaf(reg, i) }
  leaves.concat(ls)
  Ractor.new(reg, ls) do |r, kids|
    cfg, up = Ractor.receive
    inbox = Ractor::Port.new
    kids.each { |k| k.send([cfg, inbox]) }
    acks = []
    kids.size.times { acks << inbox.receive }
    raise "cross-region ack" unless acks.all? { |rr, _i, _v| rr == r }
    up.send([r, acks.map { |_rr, _i, v| v }.sum])
    :fin
  end
end
root = Ractor.new(heads) do |hs|
  cfg, up = Ractor.receive
  inbox = Ractor::Port.new
  hs.each { |h| h.send([cfg, inbox]) }
  totals = {}
  hs.size.times do
    r, s = inbox.receive
    totals[r] = s
  end
  up.send(totals)
  :fin
end
top = Ractor::Port.new
root.send([{ epoch: 7 }, top])
totals = top.receive
2.times do |r|
  exp = (0...4).sum { |i| 7 + r * 10 + i }
  raise "region #{r}" unless totals[r] == exp
end
GC.stress = false
GC.compact
([root] + heads + leaves).each { |x| raise unless x.value == :fin }
puts "OK e78_hier_bcast"
