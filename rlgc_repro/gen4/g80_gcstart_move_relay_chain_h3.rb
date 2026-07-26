# a chain of 3 ractors relays batches via move, each hop GC.start on its first messages, sink sums bytes
# axes: 3 ractors (chain), move send, GC.start, gc at relay hops
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
HOPS = 3
M = 6
sink = Ractor::Port.new
chain = sink
HOPS.times do
  nxt = chain
  chain = Ractor.new(nxt) do |dst|
    seen = 0
    loop do
      msg = Ractor.receive
      if msg == :eof
        dst.send(:eof)
        break
      end
      GC.start if seen < 1
      seen += 1
      dst.send(msg, move: true)
    end
  end
end
exp = 0
M.times do |k|
  batch = Array.new(7) { |i| +"rl#{k}-#{i}" }
  exp += batch.sum(&:bytesize)
  chain.send(batch, move: true)
end
chain.send(:eof)
got = 0
loop do
  m = sink.receive
  break if m == :eof
  got += m.sum(&:bytesize)
end
raise "got=#{got} exp=#{exp}" unless got == exp
puts "OK g80_gcstart_move_relay_chain_h3"
