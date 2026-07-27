# relay chain of 5 with move: mutable string payload appended at each hop, moved onward
# axes: move on every hop, in-place mutation, husks left at each node
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

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
      m << "-#{id}"
      nx.send(m, move: true)
    end
    :fin
  end
  chain.unshift(nxt)
end
head = chain[0]
J.times { |k| head.send("j#{k}", move: true) }
head.send(:stop)
suffix = (0...N).map { |i| "-#{i}" }.join
got = []
loop do
  m = done.receive
  break if m == :stop
  got << m
end
raise "got #{got}" unless got == (0...J).map { |k| "j#{k}#{suffix}" }
GC.stress = false
chain.each { |r| raise unless r.value == :fin }
puts "OK e26_chain_move_str"
