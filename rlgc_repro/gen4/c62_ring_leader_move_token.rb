# c62: max-uid ring election with a moved mutable token that records its path;
# starter asserts the exact circuit order.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

N = STRESS ? 4 : 6
UIDS = Ractor.make_shareable(Array.new(N) { |i| (i * 29 + 11) % 83 })

reg = Ractor::Port.new
done = Ractor::Port.new
ws = N.times.map do |i|
  Ractor.new(reg, done, i, N) do |rp, dp, id, n|
    my = Ractor::Port.new
    rp << [id, my]
    ports = Ractor.receive
    nxt = ports[(id + 1) % n]
    uid = UIDS[id]
    if id == 0
      tok = { best: uid, hops: 1, path: [0] }
      nxt.send([:tok, tok], move: true)
    end
    leader = nil
    loop do
      msg = my.receive
      case msg[0]
      when :tok
        tok = msg[1]
        if id == 0 && tok[:hops] == n
          raise "path" unless tok[:path] == (0...n).to_a
          leader = tok[:best]
          nxt << [:leader, leader]
        else
          tok[:best] = uid if uid > tok[:best]
          tok[:hops] += 1
          tok[:path] << id
          nxt.send([:tok, tok], move: true)
        end
      when :leader
        if id == 0
          break
        else
          leader = msg[1]
          nxt << [:leader, leader]
          break
        end
      end
    end
    dp << [:done, id, leader]
    :member_done
  end
end

ports = Array.new(N)
N.times do
  id, port = reg.receive
  ports[id] = port
end
ws.each { |w| w.send(ports) }

expected = UIDS.max
N.times do
  tag, _id, leader = done.receive
  raise "done" unless tag == :done && leader == expected
end
GC.stress = false
ws.each { |w| raise unless w.value == :member_done }
puts "OK c62_ring_leader_move_token"
