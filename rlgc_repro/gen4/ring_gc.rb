# gen4 ring: 6 ractors, copy transfer; every node runs GC.start every 25
# forwards and node 3 runs GC.compact every 60, all while the token is in flight.
# axes: transfer=copy, GC=GC.start all nodes + GC.compact one node, exceptions=none
N = 6
LAPS = 100

done = Ractor::Port.new
nodes = N.times.map do |i|
  Ractor.new(done, i, LAPS) do |dport, id, laps|
    nxt = Ractor.receive
    forwards = 0
    junk = []
    while (tok = Ractor.receive) != :stop
      if id == 0 && tok[:lap] == laps
        dport << tok
        next
      end
      tok[:lap] += 1 if id == 0
      tok[:hops] += 1
      junk << "j#{forwards}" * 3
      junk.clear if junk.size > 40
      forwards += 1
      GC.start if forwards % 25 == 0
      GC.compact if id == 3 && forwards % 60 == 0
      nxt << tok
    end
    forwards
  end
end
nodes.each_with_index { |n, i| n << nodes[(i + 1) % N] }

nodes[0] << { lap: 0, hops: 0 }
tok = done.receive
nodes.each { |n| n << :stop }
forwards = nodes.sum(&:value)

raise "FAIL hops" unless tok[:hops] == N * LAPS
raise "FAIL forwards" unless forwards == N * LAPS
puts "OK ring_gc"
