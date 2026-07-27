# binary tree broadcast down: root pushes config to leaves, leaves ack checksum to main port
# axes: top-down push through intermediate nodes, ack fan-in, copy payload
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

ack = Ractor::Port.new
leaves = 4.times.map do |i|
  Ractor.new(i, ack) do |id, ackp|
    tag, cfg = Ractor.receive
    raise unless tag == :bcast
    ackp.send([id, cfg[:ver] * 100 + cfg[:vals].sum])
    :fin
  end
end
mids = 2.times.map do |m|
  Ractor.new(leaves[m * 2], leaves[m * 2 + 1]) do |a, b|
    msg = Ractor.receive
    a.send(msg)
    b.send(msg)
    :fin
  end
end
root = Ractor.new(mids[0], mids[1]) do |a, b|
  msg = Ractor.receive
  a.send(msg)
  b.send(msg)
  GC.start
  :fin
end
cfg = { ver: 3, vals: [1, 2, 3, 4] }
root.send([:bcast, cfg])
got = []
4.times { got << ack.receive }
exp_ck = 3 * 100 + 10
raise unless got.map(&:first).sort == [0, 1, 2, 3]
got.each { |_id, ck| raise "bad ck #{ck}" unless ck == exp_ck }
GC.stress = false
([root] + mids + leaves).each { |r| raise unless r.value == :fin }
puts "OK e12_tree_bcast_ack"
