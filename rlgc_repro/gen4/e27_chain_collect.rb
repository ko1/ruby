# relay chain of 8: each hop pushes [id, id*id] into the message array (copy send each hop)
# axes: growing copied payload, path verification at the sink
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 8
J = 3
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
      m << [id, id * id]
      nx.send(m)
    end
    :fin
  end
  chain.unshift(nxt)
end
head = chain[0]
J.times { |k| head.send([[:job, k]]) }
head.send(:stop)
exp_tail = (0...N).map { |i| [i, i * i] }
cnt = 0
loop do
  m = done.receive
  break if m == :stop
  raise "path #{m}" unless m[0][0] == :job && m[1..] == exp_tail
  cnt += 1
end
raise unless cnt == J
GC.stress = false
GC.compact
chain.each { |r| raise unless r.value == :fin }
puts "OK e27_chain_collect"
