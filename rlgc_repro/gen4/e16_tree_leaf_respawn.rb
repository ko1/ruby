# tree lifecycle: leaves serve one request then die; main respawns fresh leaves each round
# axes: deterministic node death+respawn, parent rewired via tagged mailbox messages
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

ROUNDS = 3
report = Ractor::Port.new
parent = Ractor.new(report, ROUNDS) do |rep, rounds|
  rounds.times do
    tag, l1, l2 = Ractor.receive
    raise unless tag == :leaves
    inbox = Ractor::Port.new
    l1.send([:req, inbox])
    l2.send([:req, inbox])
    rep.send(inbox.receive + inbox.receive)
  end
  :fin
end

def spawn_leaf(gen, idx)
  Ractor.new(gen, idx) do |g, i|
    _tag, rp = Ractor.receive
    rp.send(g * 10 + i)
    :fin
  end
end

dead = []
ROUNDS.times do |g|
  l1 = spawn_leaf(g, 1)
  l2 = spawn_leaf(g, 2)
  parent.send([:leaves, l1, l2])
  got = report.receive
  raise "round #{g}: #{got}" unless got == (g * 10 + 1) + (g * 10 + 2)
  dead << l1 << l2
  GC.start if g == 1
end
GC.stress = false
dead.each { |r| raise unless r.value == :fin }
raise unless parent.value == :fin
puts "OK e16_tree_leaf_respawn"
