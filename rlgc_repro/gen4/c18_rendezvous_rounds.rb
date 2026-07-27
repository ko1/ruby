# c18: repeated rendezvous over R rounds with per-round pending queues in the
# matchmaker (rounds may overlap in arrival order); pairs-per-round asserted.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

M = STRESS ? 4 : 6   # even
R = STRESS ? 3 : 8

match = Ractor.new(M, R) do |m, rmax|
  pending = Hash.new { |h, k| h[k] = nil }
  pairs = Hash.new(0)
  (m * rmax).times do
    tag, id, round, val, port = Ractor.receive
    raise "tag" unless tag == :meet
    if (prev = pending[round])
      pid, pval, pport = prev
      pport << [:partner, id, val]
      port << [:partner, pid, pval]
      pending[round] = nil
      pairs[round] += 1
    else
      pending[round] = [id, val, port]
    end
  end
  rmax.times { |r| raise "round #{r}" unless pairs[r] == m / 2 && pending[r].nil? }
  :mdone
end

done = Ractor::Port.new
ws = M.times.map do |i|
  Ractor.new(match, done, i, R) do |mm, dp, id, rmax|
    my = Ractor::Port.new
    acc = 0
    rmax.times do |round|
      own = id * 100 + round
      mm.send([:meet, id, round, own, my])
      tag, pid, pval = my.receive
      raise "partner" unless tag == :partner && pid != id
      raise "pval" unless pval == pid * 100 + round
      acc += pval
    end
    dp << [:done, id, acc]
  end
end

total = 0
M.times do
  t, _, acc = done.receive
  raise "done" unless t == :done
  total += acc
end
# every value id*100+round is received by exactly one partner each round
expected = R.times.sum { |r| M.times.sum { |i| i * 100 + r } }
raise "total #{total}" unless total == expected
GC.stress = false
raise unless match.value == :mdone
ws.each(&:value)
puts "OK c18_rendezvous_rounds"
