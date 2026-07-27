# gen4 ring: 6 ractors; the token is an ARRAY MOVED on every hop (its slots
# mutated in place). Same lap/hop accounting as ring_copy.
# axes: transfer=move every hop, GC=none, exceptions=none, payload=array [lap,hops,sum,buf]
N = 6
LAPS = 120

done = Ractor::Port.new
nodes = N.times.map do |i|
  Ractor.new(done, i, LAPS) do |dport, id, laps|
    nxt = Ractor.receive
    forwards = 0
    while (tok = Ractor.receive) != :stop
      if id == 0 && tok[0] == laps
        dport.send(tok, move: true)
        next
      end
      tok[0] += 1 if id == 0
      tok[1] += 1
      tok[2] += id
      forwards += 1
      nxt.send(tok, move: true)
    end
    forwards
  end
end
nodes.each_with_index { |n, i| n << nodes[(i + 1) % N] }

nodes[0].send([0, 0, 0, "tokenbuf" * 4], move: true)
tok = done.receive
nodes.each { |n| n << :stop }
forwards = nodes.sum(&:value)

raise "FAIL lap" unless tok[0] == LAPS
raise "FAIL hops" unless tok[1] == N * LAPS
raise "FAIL sum" unless tok[2] == LAPS * (0...N).sum
raise "FAIL buf" unless tok[3] == "tokenbuf" * 4
raise "FAIL forwards" unless forwards == N * LAPS
puts "OK ring_move"
