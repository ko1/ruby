# c20: rendezvous exchanging Struct payloads validated against a frozen
# shareable expectation table; bounded participant GC.stress.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false

M = STRESS ? 4 : 8   # even
Pay = Struct.new(:id, :val, :tag)
EXPECT = Ractor.make_shareable(Array.new(M) { |i| i * 13 + 2 })

match = Ractor.new(M) do |m|
  pending = nil
  m.times do
    tag, s, port = Ractor.receive
    raise "tag" unless tag == :meet
    if pending
      ps, pport = pending
      pport << [:partner, s]
      port << [:partner, ps]
      pending = nil
    else
      pending = [s, port]
    end
  end
  :mdone
end

done = Ractor::Port.new
ws = M.times.map do |i|
  Ractor.new(match, done, i) do |mm, dp, id|
    GC.stress = true if ENV['S_STRESS']
    my = Ractor::Port.new
    mm.send([:meet, Pay.new(id, EXPECT[id], "t#{id}"), my])
    tag, got = my.receive
    raise "tag" unless tag == :partner
    raise "self" if got.id == id
    raise "val" unless got.val == EXPECT[got.id]
    raise "stag" unless got.tag == "t#{got.id}"
    GC.stress = false
    dp << [:done, id, got.id]
  end
end

pairs = {}
M.times do
  t, id, pid = done.receive
  raise "done" unless t == :done
  pairs[id] = pid
end
M.times { |i| raise "involution" unless pairs[pairs[i]] == i && pairs[i] != i }
raise unless match.value == :mdone
ws.each(&:value)
GC.compact unless STRESS
puts "OK c20_rendezvous_struct"
