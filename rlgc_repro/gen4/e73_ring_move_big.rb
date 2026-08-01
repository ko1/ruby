# ring N=4 moving a big token: nested hash with per-node ledgers, moved every hop, 2 laps
# axes: big moved payload, in-place ledger mutation, final structural verification
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 4
LAPS = 2
done = Ractor::Port.new
nodes = N.times.map do |i|
  Ractor.new(i, done) do |id, dport|
    nxt = Ractor.receive
    loop do
      m = Ractor.receive
      break if m == :stop
      m[:ledgers][id] << m[:hop]
      m[:hop] += 1
      m[:blob] << ("x" * 10)
      if m[:hop] >= m[:limit]
        dport.send(m, move: true)
      else
        nxt.send(m, move: true)
      end
    end
    :fin
  end
end
N.times { |i| nodes[i].send(nodes[(i + 1) % N]) }
tok = {
  hop: 0,
  limit: LAPS * N,
  blob: +"seed",
  ledgers: Array.new(N) { [] },
}
nodes[0].send(tok, move: true)
out = done.receive
N.times do |i|
  exp = (0...LAPS).map { |lap| lap * N + i }
  raise "ledger #{i}: #{out[:ledgers][i]}" unless out[:ledgers][i] == exp
end
raise "blob" unless out[:blob].length == 4 + 10 * LAPS * N
nodes.each { |r| r.send(:stop) }
GC.stress = false
nodes.each { |r| raise unless r.value == :fin }
GC.compact
puts "OK e73_ring_move_big"
