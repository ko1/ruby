# c63: hierarchical election: two sub-rings elect local minima; the two winners
# are combined by a referee ractor; global result broadcast to every member.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

H = STRESS ? 2 : 3            # members per sub-ring
N = 2 * H
UIDS = Ractor.make_shareable(Array.new(N) { |i| (i * 23 + 7) % 71 })

referee = Ractor.new do
  a = Ractor.receive
  b = Ractor.receive
  raise "ref tags" unless a[0] == :local && b[0] == :local
  winner = [a[1], b[1]].min
  a[2] << [:global, winner]
  b[2] << [:global, winner]
  winner
end

reg = Ractor::Port.new
done = Ractor::Port.new
ws = []
2.times do |ring|
  H.times do |k|
    gid = ring * H + k
    ws << Ractor.new(reg, done, referee, ring, k, H, gid) do |rp, dp, ref, rg, idx, h, id|
      my = Ractor::Port.new
      rp << [id, my]
      ports = Ractor.receive        # this sub-ring's ports (index by k)
      nxt = ports[(idx + 1) % h]
      uid = UIDS[id]
      nxt << [:tok, uid, 1] if idx == 0
      global = nil
      loop do
        msg = my.receive
        case msg[0]
        when :tok
          _, m, hops = msg
          if idx == 0 && hops == h
            ref.send([:local, m, my])       # sub-ring leader value to referee
          else
            nxt << [:tok, m < uid ? m : uid, hops + 1]
          end
        when :global
          global = msg[1]
          if idx == 0
            nxt << [:bcast, global]
          end
        when :bcast
          global = msg[1]
          nxt << [:bcast, global] unless (idx + 1) % h == 0
        end
        break if global
      end
      dp << [:done, id, global]
      :member_done
    end
  end
end

ports = Array.new(N)
N.times do
  id, port = reg.receive
  ports[id] = port
end
2.times do |ring|
  sub = ports[ring * H, H]
  H.times { |k| ws[ring * H + k].send(sub) }
end

expected = UIDS.min
N.times do
  tag, _id, g = done.receive
  raise "done" unless tag == :done
  raise "global #{g}" unless g == expected
end
GC.stress = false
raise unless referee.value == expected
ws.each { |w| raise unless w.value == :member_done }
puts "OK c63_leader_two_subrings"
