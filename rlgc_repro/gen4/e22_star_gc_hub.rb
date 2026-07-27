# star hub running GC.start every 3 messages and GC.compact once at half; spokes verify echoes
# axes: GC pressure concentrated at hub while serving
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

K = 4
R = 3
hreg = Ractor::Port.new
hub = Ractor.new(hreg, K * R) do |reg, total|
  inbox = Ractor::Port.new
  reg.send(inbox)
  total.times do |k|
    id, rp, v = inbox.receive
    GC.start if k % 3 == 0
    GC.compact if k == total / 2
    rp.send(v + 1000)
  end
  :hub_fin
end
hub_in = hreg.receive
done = Ractor::Port.new
spokes = K.times.map do |i|
  Ractor.new(i, hub_in, done, R) do |id, h, dport, rounds|
    my = Ractor::Port.new
    acc = []
    rounds.times do |r|
      h.send([id, my, id * 10 + r])
      acc << my.receive
    end
    dport.send([id, acc])
    :fin
  end
end
K.times do
  id, acc = done.receive
  exp = (0...R).map { |r| id * 10 + r + 1000 }
  raise "spoke #{id}: #{acc}" unless acc == exp
end
GC.stress = false
raise unless hub.value == :hub_fin
spokes.each { |r| raise unless r.value == :fin }
puts "OK e22_star_gc_hub"
