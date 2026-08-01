# c14: barrier where the barrier ractor itself runs GC.start each round and one
# bounded GC.compact; participants use bounded GC.stress around their compute.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false

N = STRESS ? 3 : 5
R = STRESS ? 3 : 6

barrier = Ractor.new(N, R, STRESS) do |n, rmax, st|
  rmax.times do |round|
    ports = Array.new(n)
    n.times do
      tag, id, rd, port = Ractor.receive
      raise "tag" unless tag == :arrive && rd == round
      ports[id] = port
    end
    GC.start
    GC.compact if round == 1 && !st
    ports.each { |p| p << [:go, round] }
  end
  :bdone
end

done = Ractor::Port.new
ws = N.times.map do |i|
  Ractor.new(barrier, done, i, R) do |b, dp, id, rmax|
    my = Ractor::Port.new
    acc = 0
    rmax.times do |round|
      GC.stress = true if ENV['S_STRESS']
      tmp = Array.new(16) { |x| (x + id) * (round + 1) }
      acc += tmp.sum
      GC.stress = false
      b.send([:arrive, id, round, my])
      tag, rd = my.receive
      raise "go" unless tag == :go && rd == round
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
expected = N.times.sum { |i| R.times.sum { |r| Array.new(16) { |x| (x + i) * (r + 1) }.sum } }
raise "sum" unless sum == expected
raise unless barrier.value == :bdone
ws.each(&:value)
puts "OK c14_barrier_gc_rounds"
