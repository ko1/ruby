# c03: K=1 mutex semaphore; clients respawn every round (per-phase lifecycle),
# joined via #value each round; GC.compact/GC.start between rounds.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

ROUNDS = STRESS ? 3 : 8
M = STRESS ? 3 : 4

sema = Ractor.new do
  locked = false
  waiting = []
  grants = 0
  loop do
    msg = Ractor.receive
    case msg[0]
    when :acquire
      if locked
        waiting << msg[1]
      else
        locked = true
        grants += 1
        msg[1] << :go
      end
    when :release
      raise "not locked" unless locked
      if (w = waiting.shift)
        grants += 1
        w << :go
      else
        locked = false
      end
    when :stop
      raise "locked at stop" if locked
      break grants
    end
  end
end

done = Ractor::Port.new
total = 0
ROUNDS.times do |round|
  rs = M.times.map do |i|
    Ractor.new(sema, done, i, round) do |s, dp, id, rd|
      reply = Ractor::Port.new
      s.send([:acquire, reply])
      raise "bad go" unless reply.receive == :go
      v = id * 10 + rd
      s.send([:release])
      dp << [:done, v]
      v
    end
  end
  M.times do
    t, v = done.receive
    raise "bad done" unless t == :done
    total += v
  end
  GC.stress = false if STRESS
  rs.each(&:value)
  GC.stress = true if STRESS
  if round.even?
    GC.start
  else
    GC.compact unless STRESS
  end
end
expected = ROUNDS.times.sum { |rd| M.times.sum { |i| i * 10 + rd } }
raise "total #{total} != #{expected}" unless total == expected
sema.send([:stop])
GC.stress = false
raise "grants" unless sema.value == ROUNDS * M
puts "OK c03_sema_mutex_respawn"
