# c15: rendezvous matchmaker pairs consecutive requesters; partners exchange
# values (copy). Asserts involution pairing + value multiset conservation.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

M = STRESS ? 4 : 8   # even

match = Ractor.new(M) do |m|
  pending = nil
  pairs = 0
  m.times do
    tag, id, val, port = Ractor.receive
    raise "tag" unless tag == :meet
    if pending
      pid, pval, pport = pending
      pport << [:partner, id, val]
      port << [:partner, pid, pval]
      pending = nil
      pairs += 1
    else
      pending = [id, val, port]
    end
  end
  raise "pending left" if pending
  pairs
end

done = Ractor::Port.new
ws = M.times.map do |i|
  Ractor.new(match, done, i) do |mm, dp, id|
    my = Ractor::Port.new
    own = id * 11 + 5
    mm.send([:meet, id, own, my])
    tag, pid, pval = my.receive
    raise "partner tag" unless tag == :partner
    raise "self pair" if pid == id
    dp << [:done, id, own, pid, pval]
  end
end

partner = {}
gotvals = []
M.times do
  t, id, own, pid, pval = done.receive
  raise "done" unless t == :done
  raise "own" unless own == id * 11 + 5
  raise "pval" unless pval == pid * 11 + 5
  partner[id] = pid
  gotvals << pval
end
M.times do |i|
  raise "involution" unless partner[partner[i]] == i && partner[i] != i
end
raise "multiset" unless gotvals.sort == M.times.map { |i| i * 11 + 5 }.sort
GC.stress = false
raise "pairs" unless match.value == M / 2
ws.each(&:value)
GC.start
puts "OK c15_rendezvous_pairs"
