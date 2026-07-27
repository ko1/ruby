# Chang-Roberts leader election on ring N=7: forward ids greater than own, drop smaller
# axes: algorithmic message counts precomputed by simulation, leader = max id holder
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 7
IDS = [23, 5, 61, 17, 42, 9, 30].freeze # unique station ids around the ring

# simulate: token j starts at position j, moves clockwise, dies at first station with bigger id
recv_count = Array.new(N, 0)
N.times do |j|
  tok = IDS[j]
  pos = (j + 1) % N
  loop do
    recv_count[pos] += 1
    break if IDS[pos] > tok
    break if IDS[pos] == tok # made it all the way around
    pos = (pos + 1) % N
  end
end

done = Ractor::Port.new
nodes = N.times.map do |i|
  Ractor.new(i, IDS[i], recv_count[i], done) do |pos, myid, expect, dport|
    # tokens may arrive before the wiring Ractor does: buffer them
    nxt = nil
    pending = []
    until nxt
      m = Ractor.receive
      m.is_a?(Ractor) ? nxt = m : pending << m
    end
    nxt.send(myid)
    leader = false
    handle = lambda do |tok|
      if tok == myid
        leader = true
      elsif tok > myid
        nxt.send(tok)
      end
    end
    pending.each { |t| handle.call(t) }
    (expect - pending.size).times { handle.call(Ractor.receive) }
    dport.send([pos, myid, leader])
    :fin
  end
end
N.times { |i| nodes[i].send(nodes[(i + 1) % N]) }
leaders = []
N.times do
  _pos, myid, leader = done.receive
  leaders << myid if leader
end
raise "leaders #{leaders}" unless leaders == [IDS.max]
GC.stress = false
nodes.each { |r| raise unless r.value == :fin }
puts "OK e75_ring_election"
