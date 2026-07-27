# c11: double barrier (compute barrier A, commit barrier B) per round; asserts
# strict A-then-B ordering per round on both sides.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

N = STRESS ? 3 : 5
R = STRESS ? 3 : 8

def make_barrier(n, rmax, tag_go)
  Ractor.new(n, rmax, tag_go) do |nn, rm, tg|
    rm.times do |round|
      ports = Array.new(nn)
      nn.times do
        tag, id, rd, port = Ractor.receive
        raise "tag" unless tag == :arrive && rd == round
        ports[id] = port
      end
      ports.each { |p| p << [tg, round] }
    end
    :bdone
  end
end

ba = make_barrier(N, R, :goA)
bb = make_barrier(N, R, :goB)
done = Ractor::Port.new
ws = N.times.map do |i|
  Ractor.new(ba, bb, done, i, R) do |a, b, dp, id, rmax|
    my = Ractor::Port.new
    log = []
    rmax.times do |round|
      a.send([:arrive, id, round, my])
      t1, r1 = my.receive
      raise "A" unless t1 == :goA && r1 == round
      log << [:A, round]
      b.send([:arrive, id, round, my])
      t2, r2 = my.receive
      raise "B" unless t2 == :goB && r2 == round
      log << [:B, round]
    end
    exp = rmax.times.flat_map { |r| [[:A, r], [:B, r]] }
    raise "order" unless log == exp
    dp << [:done, id]
  end
end

N.times { t, = done.receive; raise "done" unless t == :done }
GC.stress = false
raise unless ba.value == :bdone && bb.value == :bdone
ws.each(&:value)
GC.compact unless STRESS
puts "OK c11_barrier_double_phase"
