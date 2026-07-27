# star lifecycle: 3 generations of 3 spokes; each spoke sends one report to hub then dies
# axes: deterministic spoke death+respawn, hub counts generations via tagged messages
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

GENS = 3
K = 3
hreg = Ractor::Port.new
done = Ractor::Port.new
hub = Ractor.new(hreg, done, GENS, K) do |reg, dport, gens, k|
  inbox = Ractor::Port.new
  reg.send(inbox)
  gens.times do |g|
    sum = 0
    k.times do
      gg, v = inbox.receive
      raise "gen mix #{gg} != #{g}" unless gg == g
      sum += v
    end
    dport.send([g, sum])
  end
  :hub_fin
end
hub_in = hreg.receive
all_spokes = []
GENS.times do |g|
  spokes = K.times.map do |i|
    Ractor.new(g, i, hub_in) do |gen, id, h|
      h.send([gen, gen * 100 + id])
      :fin
    end
  end
  gg, sum = done.receive
  raise unless gg == g
  exp = (0...K).sum { |i| g * 100 + i }
  raise "gen #{g}: #{sum}" unless sum == exp
  all_spokes.concat(spokes)
  GC.start if g == 1
end
GC.stress = false
raise unless hub.value == :hub_fin
all_spokes.each { |r| raise unless r.value == :fin }
GC.compact
puts "OK e23_star_spoke_respawn"
