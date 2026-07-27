# c10: cyclic barrier where each participant's scratch buffer is moved to the
# barrier on arrive and moved back on release; buffer accumulates round marks.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

N = STRESS ? 3 : 5
R = STRESS ? 3 : 10

barrier = Ractor.new(N, R) do |n, rmax|
  rmax.times do |round|
    got = []
    n.times do
      tag, id, rd, port, buf = Ractor.receive
      raise "tag" unless tag == :arrive && rd == round
      buf << round
      got << [port, buf]
    end
    got.each { |port, buf| port.send([:go, buf], move: true) }
  end
  :bdone
end

done = Ractor::Port.new
ws = N.times.map do |i|
  Ractor.new(barrier, done, i, R) do |b, dp, id, rmax|
    my = Ractor::Port.new
    buf = [id]
    rmax.times do |round|
      b.send([:arrive, id, round, my, buf], move: true)
      tag, nb = my.receive
      raise "go" unless tag == :go
      buf = nb
      raise "buf id" unless buf[0] == id
      raise "buf len" unless buf.size == round + 2
    end
    raise "final buf" unless buf == [id] + (0...rmax).to_a
    dp << [:done, id]
  end
end

N.times { t, = done.receive; raise "done" unless t == :done }
GC.stress = false
raise "bar" unless barrier.value == :bdone
ws.each(&:value)
puts "OK c10_barrier_move_buffers"
