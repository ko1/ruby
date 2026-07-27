# flow-controlled multicast: publisher waits for all member acks before next round
# axes: ack fan-in to publisher's own port, 4 lockstep rounds, counted acks
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

M = 4
N = 5
reg = Ractor::Port.new
done = Ractor::Port.new
members = N.times.map do |i|
  Ractor.new(i, reg, done, M) do |id, regp, dport, m|
    inbox = Ractor::Port.new
    regp.send([id, inbox])
    m.times do |k|
      ackp, v = inbox.receive
      raise "round skew #{v}" unless v == k
      ackp.send(id)
    end
    dport.send(id)
    :fin
  end
end
ports = Array.new(N)
N.times do
  id, p = reg.receive
  ports[id] = p
end
pub = Ractor.new(ports, M, N) do |ps, m, n|
  ackp = Ractor::Port.new
  m.times do |k|
    ps.each { |p| p.send([ackp, k]) }
    acked = []
    n.times { acked << ackp.receive }
    raise "acks #{acked}" unless acked.sort == (0...n).to_a
  end
  :pub_fin
end
N.times { done.receive }
GC.stress = false
raise unless pub.value == :pub_fin
members.each { |r| raise unless r.value == :fin }
puts "OK e43_mcast_ackcount"
