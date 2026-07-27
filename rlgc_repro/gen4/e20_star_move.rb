# star with move both directions: spoke moves string to hub, hub transforms and moves back
# axes: move payload round-trip, mutable string grown at hub
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

K = 5
R = 2
hreg = Ractor::Port.new
hub = Ractor.new(hreg, K * R) do |reg, total|
  inbox = Ractor::Port.new
  reg.send(inbox)
  total.times do
    rp, s = inbox.receive
    s << "|hub"
    rp.send(s, move: true)
  end
  :hub_fin
end
hub_in = hreg.receive
done = Ractor::Port.new
spokes = K.times.map do |i|
  Ractor.new(i, hub_in, done, R) do |id, h, dport, rounds|
    my = Ractor::Port.new
    rounds.times do |r|
      h.send([my, "s#{id}r#{r}"], move: true)
      back = my.receive
      raise "bad echo #{back}" unless back == "s#{id}r#{r}|hub"
    end
    dport.send(id)
    :fin
  end
end
ids = []
K.times { ids << done.receive }
raise unless ids.sort == (0...K).to_a
GC.stress = false
raise unless hub.value == :hub_fin
spokes.each { |r| raise unless r.value == :fin }
puts "OK e20_star_move"
