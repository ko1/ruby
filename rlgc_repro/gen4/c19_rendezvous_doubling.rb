# c19: rendezvous sum-doubling: each pair exchange adds partner's counter, so the
# global sum doubles per round regardless of pairing. sum == S0 * 2^R asserted.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

M = STRESS ? 4 : 8   # even
R = STRESS ? 4 : 10

match = Ractor.new(M, R) do |m, rmax|
  pending = {}
  (m * rmax).times do
    tag, id, round, val, port = Ractor.receive
    raise "tag" unless tag == :meet
    if (prev = pending.delete(round))
      pid, pval, pport = prev
      pport << [:partner, id, val]
      port << [:partner, pid, pval]
    else
      pending[round] = [id, val, port]
    end
  end
  raise "pending" unless pending.empty?
  :mdone
end

done = Ractor::Port.new
ws = M.times.map do |i|
  Ractor.new(match, done, i, R) do |mm, dp, id, rmax|
    my = Ractor::Port.new
    counter = id + 1
    rmax.times do |round|
      mm.send([:meet, id, round, counter, my])
      tag, pid, pval = my.receive
      raise "partner" unless tag == :partner && pid != id
      counter += pval
      GC.start if round == rmax / 2 && !ENV['S_STRESS']
    end
    dp << [:done, id, counter]
  end
end

total = 0
M.times do
  t, _, c = done.receive
  raise "done" unless t == :done
  total += c
end
expected = (M * (M + 1) / 2) * (2**R)
raise "total #{total} != #{expected}" unless total == expected
GC.stress = false
raise unless match.value == :mdone
ws.each(&:value)
puts "OK c19_rendezvous_doubling"
