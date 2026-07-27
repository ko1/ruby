# gen4 ring: 8 ractors in a ring; a token hash is COPIED hop to hop for many
# laps. Node 0 counts laps and diverts the finished token to a done port.
# axes: transfer=copy, GC=none, exceptions=none, payload=small hash
N = 8
LAPS = 150

done = Ractor::Port.new
nodes = N.times.map do |i|
  Ractor.new(done, i, LAPS) do |dport, id, laps|
    nxt = Ractor.receive
    forwards = 0
    while (tok = Ractor.receive) != :stop
      if id == 0 && tok[:lap] == laps
        dport << tok
        next
      end
      tok[:lap] += 1 if id == 0
      tok[:hops] += 1
      tok[:sum] += id
      forwards += 1
      nxt << tok
    end
    forwards
  end
end
nodes.each_with_index { |n, i| n << nodes[(i + 1) % N] }

nodes[0] << { lap: 0, hops: 0, sum: 0 }
tok = done.receive
nodes.each { |n| n << :stop }
forwards = nodes.sum(&:value)

raise "FAIL lap #{tok[:lap]}" unless tok[:lap] == LAPS
raise "FAIL hops #{tok[:hops]}" unless tok[:hops] == N * LAPS
raise "FAIL sum" unless tok[:sum] == LAPS * (0...N).sum
raise "FAIL forwards #{forwards}" unless forwards == N * LAPS
puts "OK ring_copy"
