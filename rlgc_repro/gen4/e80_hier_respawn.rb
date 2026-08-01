# hierarchy lifecycle: region head serves one aggregation then dies; main respawns head per round
# axes: deterministic head death+respawn over 3 rounds, root rewired via tagged messages
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

ROUNDS = 3

def rleaf(idx, rounds)
  Ractor.new(idx, rounds) do |i, n|
    n.times do
      _tag, rp = Ractor.receive
      rp.send(i + 1)
    end
    :fin
  end
end

def rhead(gen, l0, l1)
  Ractor.new(gen, l0, l1) do |g, a, b|
    _tag, rp = Ractor.receive
    inbox = Ractor::Port.new
    a.send([:req, inbox])
    b.send([:req, inbox])
    rp.send(inbox.receive + inbox.receive + g * 1000)
    :fin
  end
end

leaves = 2.times.map { |i| rleaf(i, ROUNDS) }
top = Ractor::Port.new
root = Ractor.new(top, ROUNDS) do |up, rounds|
  rounds.times do
    tag, head = Ractor.receive
    raise unless tag == :head
    inbox = Ractor::Port.new
    head.send([:req, inbox])
    up.send(inbox.receive)
  end
  :fin
end
dead_heads = []
ROUNDS.times do |g|
  head = rhead(g, leaves[0], leaves[1])
  root.send([:head, head])
  got = top.receive
  raise "round #{g}: #{got}" unless got == (1 + 2) + g * 1000
  dead_heads << head
  GC.start if g == 1
end
GC.stress = false
GC.compact
dead_heads.each { |r| raise unless r.value == :fin }
raise unless root.value == :fin
leaves.each { |r| raise unless r.value == :fin }
puts "OK e80_hier_respawn"
