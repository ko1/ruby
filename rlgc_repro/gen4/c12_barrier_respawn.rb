# c12: persistent barrier, one-shot participants respawned every round
# (value-chain teardown each round); GC.start between rounds.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

N = STRESS ? 3 : 4
R = STRESS ? 3 : 8

barrier = Ractor.new(N, R) do |n, rmax|
  total = 0
  rmax.times do |round|
    ports = Array.new(n)
    n.times do
      tag, id, rd, port = Ractor.receive
      raise "tag" unless tag == :arrive && rd == round
      ports[id] = port
      total += 1
    end
    ports.each { |p| p << [:go, round] }
  end
  total
end

done = Ractor::Port.new
total = 0
R.times do |round|
  rs = N.times.map do |i|
    Ractor.new(barrier, done, i, round) do |b, dp, id, rd|
      my = Ractor::Port.new
      b.send([:arrive, id, rd, my])
      tag, r2 = my.receive
      raise "go" unless tag == :go && r2 == rd
      v = id + rd * 100
      dp << [:done, v]
      v
    end
  end
  N.times do
    t, v = done.receive
    raise "done" unless t == :done
    total += v
  end
  GC.stress = false if STRESS
  rs.each(&:value)
  GC.stress = true if STRESS
  GC.start if round == R / 2
end
expected = R.times.sum { |rd| N.times.sum { |i| i + rd * 100 } }
raise "total" unless total == expected
GC.stress = false
raise "arrivals" unless barrier.value == N * R
puts "OK c12_barrier_respawn"
