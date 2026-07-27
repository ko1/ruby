# c66: R successive elections on one persistent ring with rotating uid
# assignment; Struct token; starter runs bounded GC.start between rounds.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

N = STRESS ? 3 : 5
R = STRESS ? 2 : 4
UIDS = Ractor.make_shareable(Array.new(N) { |i| (i * 19 + 3) % 59 })
Tok = Struct.new(:best, :hops)

reg = Ractor::Port.new
done = Ractor::Port.new
ws = N.times.map do |i|
  Ractor.new(reg, done, i, N, R) do |rp, dp, id, n, rounds|
    my = Ractor::Port.new
    rp << [id, my]
    ports = Ractor.receive
    nxt = ports[(id + 1) % n]
    leaders = []
    rounds.times do |rd|
      uid = UIDS[(id + rd) % n]
      GC.start if id == 0 && rd == 1 && !ENV['S_STRESS']
      nxt << [:tok, Tok.new(uid, 1)] if id == 0
      loop do
        msg = my.receive
        case msg[0]
        when :tok
          tok = msg[1]
          if id == 0 && tok.hops == n
            leaders << tok.best
            nxt << [:leader, tok.best]
          else
            nxt << [:tok, Tok.new(uid < tok.best ? uid : tok.best, tok.hops + 1)]
          end
        when :leader
          if id == 0
            leaders[-1] == msg[1] or raise "starter leader"
            break
          else
            leaders << msg[1]
            nxt << [:leader, msg[1]]
            break
          end
        end
      end
    end
    dp << [:done, id, leaders]
    :member_done
  end
end

ports = Array.new(N)
N.times do
  id, port = reg.receive
  ports[id] = port
end
ws.each { |w| w.send(ports) }

expected = R.times.map { UIDS.min }   # rotation permutes assignment, min is invariant
N.times do
  tag, _id, leaders = done.receive
  raise "done" unless tag == :done
  raise "leaders #{leaders.inspect}" unless leaders == expected
end
GC.stress = false
ws.each { |w| raise unless w.value == :member_done }
puts "OK c66_leader_rotating_rounds"
