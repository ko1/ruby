# hop-limited forwarding on a 6-node line: messages carry ttl, drop node reports to main
# axes: ttl decrement per hop, deterministic drop positions, :stop cascade teardown
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 6
drops = Ractor::Port.new
tail_sink = Ractor::Port.new
nxt = tail_sink
chain = []
(N - 1).downto(0) do |i|
  nxt = Ractor.new(i, nxt, drops) do |id, nx, dr|
    loop do
      m = Ractor.receive
      if m == :stop
        nx.send(:stop)
        break
      end
      ttl, tag = m
      if ttl <= 1
        dr.send([tag, id])
      else
        nx.send([ttl - 1, tag])
      end
    end
    :fin
  end
  chain.unshift(nxt)
end
head = chain[0]
ttls = [1, 2, 4, 6, 3]
ttls.each_with_index { |t, k| head.send([t, k]) }
got = {}
ttls.size.times do
  tag, at = drops.receive
  got[tag] = at
end
ttls.each_with_index do |t, k|
  raise "msg #{k} dropped at #{got[k]}" unless got[k] == t - 1
end
head.send(:stop)
raise unless tail_sink.receive == :stop
GC.stress = false
chain.each { |r| raise unless r.value == :fin }
puts "OK e67_ttl_line"
