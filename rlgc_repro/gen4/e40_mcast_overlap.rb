# multicast with overlapping groups: members 2,3 belong to both groups and see both streams
# axes: overlapping membership, expected count differs per node, copy payload
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

M = 3
N = 6
GROUP_A = [0, 1, 2, 3].freeze
GROUP_B = [2, 3, 4, 5].freeze
reg = Ractor::Port.new
done = Ractor::Port.new
members = N.times.map do |i|
  expect = (GROUP_A.include?(i) ? M : 0) + (GROUP_B.include?(i) ? M : 0)
  Ractor.new(i, reg, done, expect) do |id, regp, dport, exp|
    inbox = Ractor::Port.new
    regp.send([id, inbox])
    tally = { a: 0, b: 0 }
    exp.times do
      g, _v = inbox.receive
      tally[g] += 1
    end
    dport.send([id, tally])
    :fin
  end
end
port_by_id = Array.new(N)
N.times do
  id, p = reg.receive
  port_by_id[id] = p
end
pub = Ractor.new(GROUP_A.map { |i| port_by_id[i] }, GROUP_B.map { |i| port_by_id[i] }, M) do |pa, pb, m|
  m.times do |k|
    pa.each { |p| p.send([:a, k]) }
    pb.each { |p| p.send([:b, k]) }
  end
  :pub_fin
end
N.times do
  id, tally = done.receive
  exp = { a: GROUP_A.include?(id) ? M : 0, b: GROUP_B.include?(id) ? M : 0 }
  raise "member #{id}: #{tally}" unless tally == exp
end
GC.stress = false
raise unless pub.value == :pub_fin
members.each { |r| raise unless r.value == :fin }
puts "OK e40_mcast_overlap"
