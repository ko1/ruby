# gen4 pipeline: fan-out/fan-in. A splitter stage round-robins items to 3
# parallel transformers which all feed one merger; merger waits for 3 :eos.
# axes: transfer=copy, GC=none, exceptions=none, payload=arrays
N_ITEMS = 450
N_PAR = 3

out = Ractor::Port.new

merger = Ractor.new(out, N_PAR) do |o, npar|
  eos = 0
  n = sum = 0
  until eos == npar
    m = Ractor.receive
    if m == :eos
      eos += 1
    else
      n += 1
      sum += m
    end
  end
  o << [n, sum]
end

transformers = N_PAR.times.map do |ti|
  Ractor.new(merger, ti) do |nxt, _id|
    while (m = Ractor.receive) != :eos
      nxt << m.sum { |x| x * x }
    end
    nxt << :eos
  end
end

splitter = Ractor.new(transformers, N_PAR) do |ts, npar|
  i = 0
  while (m = Ractor.receive) != :eos
    ts[i % npar] << m
    i += 1
  end
  ts.each { |t| t << :eos }
end

expected = 0
N_ITEMS.times do |i|
  arr = [i % 9, i % 4, 2]
  expected += arr.sum { |x| x * x }
  splitter << arr
end
splitter << :eos

n, sum = out.receive
([splitter, merger] + transformers).each(&:join)
raise "FAIL n #{n}" unless n == N_ITEMS
raise "FAIL sum #{sum} != #{expected}" unless sum == expected
puts "OK pl_fanout_fanin"
