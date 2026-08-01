# multi-token ring N=9: three tokens injected at 0,3,6, each hops 2 laps then reports
# axes: concurrent tokens on one ring, per-token sums, stop token sweeps once
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 9
HOPS = 2 * N
done = Ractor::Port.new
nodes = N.times.map do |i|
  Ractor.new(i, done) do |id, dport|
    nxt = Ractor.receive
    passes = 0
    loop do
      m = Ractor.receive
      tag = m[0]
      if tag == :stop
        nxt.send([:stop, m[1] - 1]) if m[1] > 0
        dport.send([:passes, id, passes])
        break
      end
      _t, tok, hops, acc = m
      passes += 1
      if hops > 1
        nxt.send([:tok, tok, hops - 1, acc + id])
      else
        dport.send([:done, tok, acc + id])
      end
    end
    :fin
  end
end
N.times { |i| nodes[i].send(nodes[(i + 1) % N]) }
starts = [0, 3, 6]
starts.each_with_index { |s, t| nodes[s].send([:tok, t, HOPS, 0]) }
tok_sums = {}
3.times do
  tag, tok, acc = done.receive
  raise unless tag == :done
  tok_sums[tok] = acc
end
exp = 2 * (0...N).sum
3.times { |t| raise "tok #{t}: #{tok_sums[t]}" unless tok_sums[t] == exp }
nodes[0].send([:stop, N - 1])
passes = {}
N.times do
  tag, id, c = done.receive
  raise unless tag == :passes
  passes[id] = c
end
N.times { |i| raise "node #{i}: #{passes[i]}" unless passes[i] == 3 * 2 }
GC.stress = false
nodes.each { |r| raise unless r.value == :fin }
puts "OK e72_ring_multitok"
