# c64: two election generations with full member respawn between them (value-
# chain teardown); different uid tables per generation; both leaders asserted.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false

N = STRESS ? 3 : 5
TBL = Ractor.make_shareable([
  Array.new(8) { |i| (i * 13 + 4) % 53 },
  Array.new(8) { |i| (i * 31 + 9) % 67 },
])

2.times do |gen|
  GC.stress = true if STRESS && gen == 0
  reg = Ractor::Port.new
  done = Ractor::Port.new
  ws = N.times.map do |i|
    Ractor.new(reg, done, i, N, gen) do |rp, dp, id, n, g|
      GC.stress = true if ENV['S_STRESS'] && g > 0
      my = Ractor::Port.new
      rp << [id, my]
      ports = Ractor.receive
      nxt = ports[(id + 1) % n]
      uid = TBL[g][id]
      nxt << [:tok, uid, 1] if id == 0
      leader = nil
      loop do
        msg = my.receive
        case msg[0]
        when :tok
          _, m, hops = msg
          if id == 0 && hops == n
            leader = m
            nxt << [:leader, m]
          else
            nxt << [:tok, m < uid ? m : uid, hops + 1]
          end
        when :leader
          if id == 0
            break
          else
            leader = msg[1]
            nxt << [:leader, leader]
            break
          end
        end
      end
      GC.stress = false
      dp << [:done, id, leader]
      leader
    end
  end

  ports = Array.new(N)
  N.times do
    id, port = reg.receive
    ports[id] = port
  end
  ws.each { |w| w.send(ports) }

  expected = TBL[gen][0, N].min
  N.times do
    tag, _id, leader = done.receive
    raise "done" unless tag == :done && leader == expected
  end
  GC.stress = false
  ws.each { |w| raise unless w.value == expected }
  GC.start if gen == 0 && !STRESS
end
puts "OK c64_leader_two_generations"
