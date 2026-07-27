# bidirectional ring N=6: cw and ccw tokens circulate concurrently with hop budgets
# axes: two directions on one ring, per-direction stop tokens, pass counts verified
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 6
L = 2
done = Ractor::Port.new
nodes = N.times.map do |i|
  Ractor.new(i, done) do |id, dport|
    tag, nxt, prv = Ractor.receive
    raise unless tag == :wire
    stops = 0
    cw = 0
    ccw = 0
    while stops < 2
      m = Ractor.receive
      dir, hops, acc = m
      case dir
      when :stop_cw
        nxt.send([:stop_cw, hops - 1, 0]) if hops > 0
        stops += 1
      when :stop_ccw
        prv.send([:stop_ccw, hops - 1, 0]) if hops > 0
        stops += 1
      when :cw
        cw += 1
        hops > 1 ? nxt.send([:cw, hops - 1, acc + id]) : dport.send([:cw, acc + id])
      when :ccw
        ccw += 1
        hops > 1 ? prv.send([:ccw, hops - 1, acc + id]) : dport.send([:ccw, acc + id])
      end
    end
    dport.send([:counts, id, cw, ccw])
    :fin
  end
end
N.times { |i| nodes[i].send([:wire, nodes[(i + 1) % N], nodes[(i - 1) % N]]) }
nodes[0].send([:cw, L * N, 0])
nodes[0].send([:ccw, L * N, 0])
sums = {}
2.times do
  dir, acc = done.receive
  sums[dir] = acc
end
exp = L * (0...N).sum
raise "cw #{sums[:cw]}" unless sums[:cw] == exp
raise "ccw #{sums[:ccw]}" unless sums[:ccw] == exp
nodes[0].send([:stop_cw, N - 1, 0])
nodes[0].send([:stop_ccw, N - 1, 0])
counts = {}
N.times do
  tag, id, cw, ccw = done.receive
  raise unless tag == :counts
  counts[id] = [cw, ccw]
end
N.times { |i| raise "node #{i}: #{counts[i]}" unless counts[i] == [L, L] }
GC.stress = false
nodes.each { |r| raise unless r.value == :fin }
puts "OK e71_ring_bidir"
