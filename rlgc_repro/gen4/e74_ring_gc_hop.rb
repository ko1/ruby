# ring N=6 where every hop triggers GC.start (even ids) or GC.compact (id 3, once per lap)
# axes: GC/compact on the token path itself, 2 laps, copy token
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 6
LAPS = 2
done = Ractor::Port.new
nodes = N.times.map do |i|
  Ractor.new(i, done) do |id, dport|
    nxt = Ractor.receive
    compacted = 0
    loop do
      m = Ractor.receive
      break if m == :stop
      hop, acc = m
      GC.start if id.even?
      if id == 3 && compacted < 1
        GC.compact
        compacted += 1
      end
      if hop + 1 >= LAPS * N
        dport.send(acc + id)
      else
        nxt.send([hop + 1, acc + id])
      end
    end
    :fin
  end
end
N.times { |i| nodes[i].send(nodes[(i + 1) % N]) }
nodes[0].send([0, 0])
acc = done.receive
raise "acc #{acc}" unless acc == LAPS * (0...N).sum
nodes.each { |r| r.send(:stop) }
GC.stress = false
nodes.each { |r| raise unless r.value == :fin }
puts "OK e74_ring_gc_hop"
