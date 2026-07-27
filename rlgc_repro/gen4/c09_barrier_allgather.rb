# c09: barrier + allgather: each arrival carries f(id,round); barrier broadcasts
# the gathered vector; every participant verifies the full vector deterministically.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

N = STRESS ? 3 : 5
R = STRESS ? 3 : 10

barrier = Ractor.new(N, R) do |n, rmax|
  rmax.times do |round|
    ports = Array.new(n)
    vals = Array.new(n)
    n.times do
      tag, id, rd, v, port = Ractor.receive
      raise "tag" unless tag == :arrive && rd == round
      ports[id] = port
      vals[id] = v
    end
    ports.each { |p| p << [:go, round, vals] }
  end
  :bdone
end

done = Ractor::Port.new
ws = N.times.map do |i|
  Ractor.new(barrier, done, i, R, N) do |b, dp, id, rmax, n|
    my = Ractor::Port.new
    rmax.times do |round|
      b.send([:arrive, id, round, id * 100 + round, my])
      tag, rd, vals = my.receive
      raise "go" unless tag == :go && rd == round
      exp = Array.new(n) { |j| j * 100 + round }
      raise "vector #{vals.inspect}" unless vals == exp
    end
    dp << [:done, id]
  end
end

N.times { t, = done.receive; raise "done" unless t == :done }
GC.stress = false
raise "bar" unless barrier.value == :bdone
ws.each(&:value)
puts "OK c09_barrier_allgather"
