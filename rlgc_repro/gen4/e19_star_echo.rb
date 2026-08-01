# star: 1 hub + 6 spokes; spokes send requests to hub port, hub echoes doubled to spoke ports
# axes: request/response star, copy payload, 3 rounds per spoke
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

K = 6
R = 3
hreg = Ractor::Port.new
hub = Ractor.new(hreg, K * R) do |reg, total|
  inbox = Ractor::Port.new
  reg.send(inbox)
  total.times do
    id, rp, v = inbox.receive
    rp.send([id, v * 2])
  end
  :hub_fin
end
hub_in = hreg.receive
done = Ractor::Port.new
spokes = K.times.map do |i|
  Ractor.new(i, hub_in, done, R) do |id, h, dport, rounds|
    my = Ractor::Port.new
    acc = 0
    rounds.times do |r|
      h.send([id, my, id * 10 + r])
      rid, v = my.receive
      raise "wrong id" unless rid == id
      acc += v
    end
    dport.send([id, acc])
    :fin
  end
end
K.times do
  id, acc = done.receive
  exp = (0...R).sum { |r| (id * 10 + r) * 2 }
  raise "spoke #{id}" unless acc == exp
end
GC.stress = false
raise unless hub.value == :hub_fin
spokes.each { |r| raise unless r.value == :fin }
puts "OK e19_star_echo"
