# relay chain of 6 nodes built tail-first; each hop adds its id; :stop cascades teardown
# axes: copy payload, per-hop transform, tail feeds main's done port
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 6
J = 5
done = Ractor::Port.new
nxt = done
chain = []
(N - 1).downto(0) do |i|
  nxt = Ractor.new(i, nxt) do |id, nx|
    loop do
      m = Ractor.receive
      if m == :stop
        nx.send(:stop)
        break
      end
      nx.send(m + id)
    end
    :fin
  end
  chain.unshift(nxt)
end
head = chain[0]
J.times { |k| head.send(k * 100) }
head.send(:stop)
add = (0...N).sum
got = []
loop do
  m = done.receive
  break if m == :stop
  got << m
end
raise "got #{got}" unless got == (0...J).map { |k| k * 100 + add }
GC.stress = false
chain.each { |r| raise unless r.value == :fin }
puts "OK e25_chain_inc"
