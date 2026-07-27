# multicast driven by a frozen shareable group table (make_shareable) passed to the publisher
# axes: shareable routing table constant, 3 groups over 9 members, copy payload
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

M = 2
N = 9
TABLE = Ractor.make_shareable({ g0: [0, 3, 6], g1: [1, 4, 7], g2: [2, 5, 8] }.freeze)
reg = Ractor::Port.new
done = Ractor::Port.new
members = N.times.map do |i|
  Ractor.new(i, reg, done, M) do |id, regp, dport, m|
    inbox = Ractor::Port.new
    regp.send([id, inbox])
    sum = 0
    m.times do
      gname, v = inbox.receive
      raise "wrong group" unless TABLE[gname].include?(id)
      sum += v
    end
    dport.send([id, sum])
    :fin
  end
end
ports = Array.new(N)
N.times do
  id, p = reg.receive
  ports[id] = p
end
pub = Ractor.new(ports, M) do |ps, m|
  m.times do |k|
    TABLE.each do |gname, ids|
      ids.each { |i| ps[i].send([gname, k + 10]) }
    end
  end
  :pub_fin
end
N.times do
  id, sum = done.receive
  raise "member #{id}" unless sum == (0...M).sum { |k| k + 10 }
end
GC.stress = false
raise unless pub.value == :pub_fin
members.each { |r| raise unless r.value == :fin }
puts "OK e44_mcast_table"
