# c46: fresh one-shot future ports + respawned producers every round
# (lifecycle churn axis); bounded main stress on round 0 only (CHECK build).
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false

ROUNDS = STRESS ? 3 : 6
N = STRESS ? 2 : 4

ROUNDS.times do |rd|
  GC.stress = true if STRESS && rd == 0
  futs = N.times.map { Ractor::Port.new }
  prods = N.times.map do |i|
    Ractor.new(futs[i], i, rd) do |fut, id, r|
      GC.stress = true if ENV['S_STRESS'] && r > 0
      v = id * 1000 + r
      GC.stress = false
      fut << [:resolved, v]
      :produced
    end
  end
  N.times do |i|
    tag, v = futs[i].receive
    raise "res" unless tag == :resolved && v == i * 1000 + rd
  end
  GC.stress = false
  prods.each { |r| raise unless r.value == :produced }
  GC.start if rd == ROUNDS - 1 && !STRESS
end
puts "OK c46_future_rounds_respawn"
