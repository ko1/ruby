# hop-limited token on a ring of 5: ttl spans multiple laps, drop node = (start+ttl-1)%N
# axes: multi-lap ttl arithmetic, ring wiring via mailbox before injection
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 5
drops = Ractor::Port.new
nodes = N.times.map do |i|
  Ractor.new(i, drops) do |id, dr|
    nxt = Ractor.receive
    loop do
      m = Ractor.receive
      if m == :stop
        break
      end
      ttl, tag, hops = m
      if ttl <= 1
        dr.send([tag, id, hops + 1])
      else
        nxt.send([ttl - 1, tag, hops + 1])
      end
    end
    :fin
  end
end
N.times { |i| nodes[i].send(nodes[(i + 1) % N]) }
cases = [[3, 0], [7, 1], [12, 2]]
cases.each { |ttl, tag| nodes[0].send([ttl, tag, 0]) }
got = {}
cases.size.times do
  tag, at, hops = got_m = drops.receive
  got[tag] = [at, hops]
end
cases.each do |ttl, tag|
  exp_at = (0 + ttl - 1) % N
  raise "case #{tag}: #{got[tag]}" unless got[tag] == [exp_at, ttl]
end
nodes.each { |r| r.send(:stop) }
GC.stress = false
nodes.each { |r| raise unless r.value == :fin }
puts "OK e68_ttl_ring"
