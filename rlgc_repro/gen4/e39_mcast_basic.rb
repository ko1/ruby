# multicast: publisher fans out to group A (3 members) and group B (4 members), M msgs each
# axes: group port lists as publisher args, copy payload, per-member count+sum check
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

M = 4
reg = Ractor::Port.new
done = Ractor::Port.new
members = 7.times.map do |i|
  group = i < 3 ? :a : :b
  Ractor.new(i, group, reg, done, M) do |id, grp, regp, dport, m|
    inbox = Ractor::Port.new
    regp.send([id, grp, inbox])
    sum = 0
    m.times do
      g, v = inbox.receive
      raise "wrong group" unless g == grp
      sum += v
    end
    dport.send([id, sum])
    :fin
  end
end
ports_a = []
ports_b = []
7.times do
  _id, grp, p = reg.receive
  (grp == :a ? ports_a : ports_b) << p
end
pub = Ractor.new(ports_a, ports_b, M) do |pa, pb, m|
  m.times do |k|
    pa.each { |p| p.send([:a, 100 + k]) }
    pb.each { |p| p.send([:b, 200 + k]) }
  end
  :pub_fin
end
exp_a = (0...M).sum { |k| 100 + k }
exp_b = (0...M).sum { |k| 200 + k }
7.times do
  id, sum = done.receive
  raise "member #{id}" unless sum == (id < 3 ? exp_a : exp_b)
end
GC.stress = false
raise unless pub.value == :pub_fin
members.each { |r| raise unless r.value == :fin }
puts "OK e39_mcast_basic"
