# gen4 ring: 7 ractors with THREE tokens circulating concurrently (staggered
# injection); node 0 retires each token after its laps; copy transfer.
# axes: transfer=copy, GC=none, exceptions=none, payload=3 small hashes in flight
N = 7
LAPS = 80
N_TOK = 3

done = Ractor::Port.new
nodes = N.times.map do |i|
  Ractor.new(done, i, LAPS) do |dport, id, laps|
    nxt = Ractor.receive
    forwards = 0
    while (tok = Ractor.receive) != :stop
      if id == 0 && tok[:lap] == laps
        dport << tok
        next
      end
      tok[:lap] += 1 if id == 0
      tok[:hops] += 1
      forwards += 1
      nxt << tok
    end
    forwards
  end
end
nodes.each_with_index { |n, i| n << nodes[(i + 1) % N] }

N_TOK.times { |t| nodes[t * 2] << { id: t, lap: 0, hops: 0 } }

got = {}
N_TOK.times do
  tok = done.receive
  got[tok[:id]] = tok
end
nodes.each { |n| n << :stop }
forwards = nodes.sum(&:value)

raise "FAIL tokens #{got.keys}" unless got.keys.sort == (0...N_TOK).to_a
# token t injected at node t*2: (N - t*2) % N hops to first reach node 0, then
# exactly N hops per lap for LAPS laps (retirement arrival adds no hop).
got.each do |t, tok|
  exp = (N - t * 2) % N + N * LAPS
  raise "FAIL hops tok#{t}: #{tok[:hops]} vs #{exp}" unless tok[:hops] == exp
end
raise "FAIL forwards" unless forwards == got.values.sum { |tok| tok[:hops] }
puts "OK ring_multi_token"
