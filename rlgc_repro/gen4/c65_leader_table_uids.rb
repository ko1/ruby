# c65: min-uid ring election where uids come from a frozen shareable config of
# records (hash rows); participant-bounded GC.stress during token handling.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false

N = STRESS ? 4 : 6
CFG = Ractor.make_shareable(Array.new(N) { |i| { node: "n#{i}", uid: (i * 41 + 13) % 97 } })

reg = Ractor::Port.new
done = Ractor::Port.new
ws = N.times.map do |i|
  Ractor.new(reg, done, i, N) do |rp, dp, id, n|
    my = Ractor::Port.new
    rp << [id, my]
    ports = Ractor.receive
    GC.stress = true if ENV['S_STRESS']
    nxt = ports[(id + 1) % n]
    uid = CFG[id][:uid]
    raise "cfg" unless CFG[id][:node] == "n#{id}"
    nxt << [:tok, uid, 1] if id == 0
    leader = nil
    loop do
      msg = my.receive
      case msg[0]
      when :tok
        _, m, hops = msg
        if id == 0 && hops == n
          leader = m
          nxt << [:leader, m]
        else
          nxt << [:tok, m < uid ? m : uid, hops + 1]
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
    GC.stress = false
    dp << [:done, id, leader]
    leader
  end
end

ports = Array.new(N)
N.times do
  id, port = reg.receive
  ports[id] = port
end
ws.each { |w| w.send(ports) }

expected = CFG.map { |r| r[:uid] }.min
N.times do
  tag, _id, leader = done.receive
  raise "done" unless tag == :done && leader == expected
end
ws.each { |w| raise unless w.value == expected }
GC.compact unless STRESS
puts "OK c65_leader_table_uids"
