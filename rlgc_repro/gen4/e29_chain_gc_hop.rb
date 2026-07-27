# relay chain of 6 with GC.start at odd hops per message and GC.compact at the tail node
# axes: GC interleaved with relay traffic on every message
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 6
J = 3
done = Ractor::Port.new
nxt = done
chain = []
(N - 1).downto(0) do |i|
  nxt = Ractor.new(i, nxt, N) do |id, nx, n|
    loop do
      m = Ractor.receive
      if m == :stop
        GC.compact if id == n - 1
        nx.send(:stop)
        break
      end
      GC.start if id.odd?
      nx.send(m + (1 << id))
    end
    :fin
  end
  chain.unshift(nxt)
end
head = chain[0]
J.times { head.send(0) }
head.send(:stop)
mask = (1 << N) - 1
cnt = 0
loop do
  m = done.receive
  break if m == :stop
  raise "mask #{m}" unless m == mask
  cnt += 1
end
raise unless cnt == J
GC.stress = false
chain.each { |r| raise unless r.value == :fin }
puts "OK e29_chain_gc_hop"
