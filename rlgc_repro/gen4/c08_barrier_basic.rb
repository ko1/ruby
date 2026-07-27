# c08: N-party cyclic barrier, R rounds; barrier ractor collects N arrivals then
# broadcasts :go; lockstep asserted via round tags. Copy payloads; stress in main.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

N = STRESS ? 3 : 6
R = STRESS ? 3 : 12

barrier = Ractor.new(N, R) do |n, rmax|
  total = 0
  rmax.times do |round|
    ports = Array.new(n)
    n.times do
      tag, id, rd, port = Ractor.receive
      raise "tag" unless tag == :arrive
      raise "lockstep: got round #{rd} at #{round}" unless rd == round
      raise "dup arrival #{id}" if ports[id]
      ports[id] = port
      total += 1
    end
    ports.each { |p| p << [:go, round] }
  end
  total
end

done = Ractor::Port.new
ws = N.times.map do |i|
  Ractor.new(barrier, done, i, R) do |b, dp, id, rmax|
    my = Ractor::Port.new
    acc = 0
    rmax.times do |round|
      b.send([:arrive, id, round, my])
      tag, rd = my.receive
      raise "go" unless tag == :go && rd == round
      acc += (id + 1) * (round + 1)
    end
    dp << [:done, id, acc]
  end
end

sum = 0
N.times do
  t, _, acc = done.receive
  raise "done" unless t == :done
  sum += acc
end
expected = N.times.sum { |i| R.times.sum { |r| (i + 1) * (r + 1) } }
raise "sum #{sum}" unless sum == expected
GC.stress = false
raise "arrivals" unless barrier.value == N * R
ws.each(&:value)
GC.start
puts "OK c08_barrier_basic"
