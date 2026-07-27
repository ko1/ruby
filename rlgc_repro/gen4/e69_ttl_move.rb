# hop-limited forwarding with moved payload: mutable trace array rides along until ttl death
# axes: move per hop on a 5-node line, trace verified at drop point
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 5
drops = Ractor::Port.new
sink = Ractor::Port.new
nxt = sink
chain = []
(N - 1).downto(0) do |i|
  nxt = Ractor.new(i, nxt, drops) do |id, nx, dr|
    loop do
      m = Ractor.receive
      if m == :stop
        nx.send(:stop)
        break
      end
      m[:trace] << id
      m[:ttl] -= 1
      if m[:ttl] <= 0
        dr.send(m, move: true)
      else
        nx.send(m, move: true)
      end
    end
    :fin
  end
  chain.unshift(nxt)
end
head = chain[0]
[2, 4, 5].each_with_index { |t, k| head.send({ ttl: t, tag: k, trace: [] }, move: true) }
got = {}
3.times do
  m = drops.receive
  got[m[:tag]] = m[:trace]
end
[2, 4, 5].each_with_index do |t, k|
  raise "tag #{k}: #{got[k]}" unless got[k] == (0...t).to_a
end
head.send(:stop)
raise unless sink.receive == :stop
GC.stress = false
chain.each { |r| raise unless r.value == :fin }
puts "OK e69_ttl_move"
