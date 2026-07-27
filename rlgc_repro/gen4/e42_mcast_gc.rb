# multicast with GC: publisher GC.start between rounds, members GC.compact after intake
# axes: GC at both ends of a fan-out, string payloads
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

M = 3
N = 4
reg = Ractor::Port.new
done = Ractor::Port.new
members = N.times.map do |i|
  Ractor.new(i, reg, done, M) do |id, regp, dport, m|
    inbox = Ractor::Port.new
    regp.send([id, inbox])
    parts = []
    m.times { parts << inbox.receive }
    GC.compact if id.even?
    dport.send([id, parts.join("+")])
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
    ps.each { |p| p.send("msg#{k}") }
    GC.start
  end
  :pub_fin
end
exp = (0...M).map { |k| "msg#{k}" }.join("+")
N.times do
  id, joined = done.receive
  raise "member #{id}: #{joined}" unless joined == exp
end
GC.stress = false
raise unless pub.value == :pub_fin
members.each { |r| raise unless r.value == :fin }
puts "OK e42_mcast_gc"
