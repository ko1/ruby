# relay chain of 5 transforming a Struct: acc updated, hops counted, trace string extended
# axes: Struct payload rebuilt per hop (copy semantics), sink verifies all fields
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

Tok = Struct.new(:acc, :hops, :trace)

N = 5
J = 4
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
      nx.send(Tok.new(m.acc * 2 + id, m.hops + 1, m.trace + id.to_s))
    end
    :fin
  end
  chain.unshift(nxt)
end
head = chain[0]
J.times { |k| head.send(Tok.new(k, 0, "")) }
head.send(:stop)
sim = ->(k) do
  acc = k
  (0...N).each { |i| acc = acc * 2 + i }
  acc
end
cnt = 0
loop do
  m = done.receive
  break if m == :stop
  raise "acc" unless m.acc == sim.call(cnt)
  raise "hops" unless m.hops == N
  raise "trace" unless m.trace == "01234"
  cnt += 1
end
raise unless cnt == J
GC.stress = false
chain.each { |r| raise unless r.value == :fin }
puts "OK e28_chain_struct"
