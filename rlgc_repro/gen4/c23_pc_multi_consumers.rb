# c23: bounded buffer, 1 producer, K consumers; deterministic round-robin by
# seq % K; consumers validate against a frozen shareable item table.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

K = STRESS ? 2 : 3
C = 3
T = (STRESS ? 6 : 30)   # total items, multiple of K
ITEMS = Ractor.make_shareable(Array.new(T) { |s| "itm#{s * 3}" })

buffer = Ractor.new(T, C, K) do |t, cap, k|
  cports = Array.new(k)
  stash = []
  regs = 0
  while regs < k                      # tolerate early items during registration
    msg = Ractor.receive
    if msg[0] == :consumer
      cports[msg[1]] = msg[2]
      regs += 1
    else
      stash << msg
    end
  end
  in_flight = 0
  items = 0
  acks = 0
  until items == t && acks == t
    msg = stash.shift || Ractor.receive
    case msg[0]
    when :item
      seq = msg[1]
      in_flight += 1
      items += 1
      raise "overflow" if in_flight > cap
      cports[seq % k] << [:item, seq, msg[2]]
    when :ack
      acks += 1
      in_flight -= 1
      msg[1] << :credit
    else
      raise "msg"
    end
  end
  raise "end" unless in_flight == 0
  :bdone
end

done = Ractor::Port.new
cons = K.times.map do |ci|
  Ractor.new(buffer, done, ci, T, K) do |b, dp, id, t, k|
    my = Ractor::Port.new
    b.send([:consumer, id, my])
    expect = id
    cnt = 0
    (t / k).times do
      tag, seq, cp = my.receive
      raise "tag" unless tag == :item
      raise "rr #{seq} != #{expect}" unless seq == expect
      raise "table" unless ITEMS[seq] == "itm#{seq * 3}"
      expect += k
      cnt += 1
      b.send([:ack, cp])
    end
    dp << [:cdone, id, cnt]
  end
end

prod = Ractor.new(buffer, done, T, C) do |b, dp, t, cap|
  credit = Ractor::Port.new
  credits = cap
  got = 0
  t.times do |seq|
    if credits == 0
      raise "credit" unless credit.receive == :credit
      credits += 1
      got += 1
    end
    b.send([:item, seq, credit])
    credits -= 1
  end
  (t - got).times { raise "drain" unless credit.receive == :credit }
  dp << [:pdone, t]
end

cnt = 0
(K + 1).times do
  m = done.receive
  cnt += m[2] if m[0] == :cdone
end
raise "cnt" unless cnt == T
GC.stress = false
raise unless buffer.value == :bdone
prod.value; cons.each(&:value)
puts "OK c23_pc_multi_consumers"
