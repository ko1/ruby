# c16: rendezvous with moved payloads: client moves a mutable array to the
# matchmaker; matchmaker moves it on to the partner; partner marks and moves to main.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

M = STRESS ? 4 : 8   # even

match = Ractor.new(M) do |m|
  pending = nil
  m.times do
    msg = Ractor.receive          # [:meet, id, port, payload] (moved in)
    raise "tag" unless msg[0] == :meet
    if pending
      pid, pport, ppay = pending
      pport.send([:partner, msg[3]], move: true)
      port2 = msg[2]
      port2.send([:partner, ppay], move: true)
      pending = nil
    else
      pending = [msg[1], msg[2], msg[3]]
    end
  end
  :mdone
end

done = Ractor::Port.new
ws = M.times.map do |i|
  Ractor.new(match, done, i) do |mm, dp, id|
    my = Ractor::Port.new
    payload = [id, "payload-#{id}", [id, id * 2]]
    mm.send([:meet, id, my, payload], move: true)
    tag, got = my.receive
    raise "tag" unless tag == :partner
    raise "self" if got[0] == id
    raise "shape" unless got[1] == "payload-#{got[0]}" && got[2] == [got[0], got[0] * 2]
    got << [:seen_by, id]
    dp.send([:done, id, got], move: true)
  end
end

ids = []
M.times do
  t, id, got = done.receive
  raise "done" unless t == :done
  raise "mark" unless got.last == [:seen_by, id]
  ids << got[0]
end
raise "multiset" unless ids.sort == (0...M).to_a
GC.stress = false
raise unless match.value == :mdone
ws.each(&:value)
puts "OK c16_rendezvous_move"
