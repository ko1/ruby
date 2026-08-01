# multicast with move: publisher builds a fresh array per member per round and moves it
# axes: move payload fan-out (one object per recipient), members mutate received arrays
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

M = 3
N = 5
reg = Ractor::Port.new
done = Ractor::Port.new
members = N.times.map do |i|
  Ractor.new(i, reg, done, M) do |id, regp, dport, m|
    inbox = Ractor::Port.new
    regp.send([id, inbox])
    acc = []
    m.times do
      a = inbox.receive
      a << id
      acc << a.sum
    end
    dport.send([id, acc])
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
    ps.each_with_index do |p, idx|
      p.send([k * 10, idx * 100], move: true)
    end
  end
  :pub_fin
end
N.times do
  id, acc = done.receive
  exp = (0...M).map { |k| k * 10 + id * 100 + id }
  raise "member #{id}: #{acc}" unless acc == exp
end
GC.stress = false
raise unless pub.value == :pub_fin
members.each { |r| raise unless r.value == :fin }
puts "OK e41_mcast_move"
