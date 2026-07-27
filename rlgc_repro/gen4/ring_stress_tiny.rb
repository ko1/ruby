# gen4 ring: tiny 4-node ring where ONE node runs under GC.stress=true; only a
# few laps so the stressed node stays fast. Copy and move alternate per lap.
# axes: transfer=copy+move alternating, GC=GC.stress in 1 node, exceptions=none
N = 4
LAPS = 12

done = Ractor::Port.new
nodes = N.times.map do |i|
  Ractor.new(done, i, LAPS) do |dport, id, laps|
    GC.stress = true if id == 2
    nxt = Ractor.receive
    forwards = 0
    while (tok = Ractor.receive) != :stop
      if id == 0 && tok[:lap] == laps
        GC.stress = false if id == 2
        dport << tok
        next
      end
      tok[:lap] += 1 if id == 0
      tok[:hops] += 1
      forwards += 1
      if tok[:lap].odd?
        nxt << tok
      else
        nxt.send(tok, move: true)
      end
    end
    GC.stress = false if id == 2
    forwards
  end
end
nodes.each_with_index { |n, i| n << nodes[(i + 1) % N] }

nodes[0] << { lap: 0, hops: 0, pad: "x" * 32 }
tok = done.receive
nodes.each { |n| n << :stop }
forwards = nodes.sum(&:value)

raise "FAIL hops" unless tok[:hops] == N * LAPS
raise "FAIL forwards" unless forwards == N * LAPS
raise "FAIL pad" unless tok[:pad] == "x" * 32
puts "OK ring_stress_tiny"
