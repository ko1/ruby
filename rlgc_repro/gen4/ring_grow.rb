# gen4 ring: 5 ractors; token is a STRING MOVED each hop and each node appends
# its marker in place, so the buffer grows continuously (embedded->heap->large).
# axes: transfer=move, GC=GC.start in one node every 50 forwards, exceptions=none, payload=growing string
N = 5
LAPS = 60

done = Ractor::Port.new
nodes = N.times.map do |i|
  Ractor.new(done, i, LAPS) do |dport, id, laps|
    nxt = Ractor.receive
    forwards = 0
    while (tok = Ractor.receive) != :stop
      lap = tok[0, 8].to_i
      if id == 0 && lap == laps
        dport.send(tok, move: true)
        next
      end
      if id == 0
        tok[0, 8] = format("%08d", lap + 1)
      end
      tok << ".#{id}"
      forwards += 1
      GC.start if id == 2 && forwards % 50 == 0
      nxt.send(tok, move: true)
    end
    forwards
  end
end
nodes.each_with_index { |n, i| n << nodes[(i + 1) % N] }

nodes[0].send(format("%08d", 0), move: true)
tok = done.receive
nodes.each { |n| n << :stop }
forwards = nodes.sum(&:value)

raise "FAIL forwards" unless forwards == N * LAPS
# ".X" appended once per forward
raise "FAIL len #{tok.size}" unless tok.size == 8 + 2 * N * LAPS
raise "FAIL lap header" unless tok[0, 8].to_i == LAPS
counts = Hash.new(0)
tok[8..].scan(/\.(\d)/) { |d| counts[d[0].to_i] += 1 }
raise "FAIL per-node #{counts}" unless counts == (0...N).to_h { |i| [i, LAPS] }
puts "OK ring_grow"
