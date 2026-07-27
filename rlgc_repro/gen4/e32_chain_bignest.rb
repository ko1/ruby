# chain of 4 moving one big nested record; each hop mutates its own section in place
# axes: big moved payload, per-hop in-place mutation, sink verifies all sections
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 4
done = Ractor::Port.new
nxt = done
chain = []
(N - 1).downto(0) do |i|
  nxt = Ractor.new(i, nxt) do |id, nx|
    rec = Ractor.receive
    sec = rec[:sections][id]
    sec[:stamped] = true
    sec[:sum] = sec[:data].sum
    rec[:log] << "n#{id}"
    nx.send(rec, move: true)
    :fin
  end
  chain.unshift(nxt)
end
rec = {
  log: [],
  sections: N.times.map { |i| { data: (1..30).map { |x| x * (i + 1) }, stamped: false, sum: nil } },
}
chain[0].send(rec, move: true)
out = done.receive
raise "log #{out[:log]}" unless out[:log] == N.times.map { |i| "n#{i}" }
N.times do |i|
  sec = out[:sections][i]
  raise "sec #{i}" unless sec[:stamped] && sec[:sum] == (1..30).sum * (i + 1)
end
GC.stress = false
chain.each { |r| raise unless r.value == :fin }
GC.compact
puts "OK e32_chain_bignest"
