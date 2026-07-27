# c25: bounded buffer with batched acknowledgements (consumer acks every B items,
# C >= B avoids deadlock); hash payloads; in-flight ceiling asserted.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

C = 4
B = 2
T = STRESS ? 8 : 32   # multiple of B

buffer = Ractor.new(T, C, B) do |t, cap, batch|
  tag, cport = Ractor.receive
  raise "reg" unless tag == :consumer
  in_flight = 0
  max_if = 0
  items = 0
  acked = 0
  until items == t && acked == t
    msg = Ractor.receive
    case msg[0]
    when :item
      in_flight += 1
      items += 1
      max_if = in_flight if in_flight > max_if
      raise "overflow #{in_flight}" if in_flight > cap
      cport << [:item, msg[1], msg[2]]
    when :ack_batch
      n, cp = msg[1], msg[2]
      raise "batch size" unless n == batch
      acked += n
      in_flight -= n
      raise "underflow" if in_flight < 0
      n.times { cp << :credit }
    end
  end
  raise "end" unless in_flight == 0
  max_if
end

reg = Ractor::Port.new
done = Ractor::Port.new
cons = Ractor.new(reg, done, T, B) do |rp, dp, t, batch|
  my = Ractor::Port.new
  rp << my
  b = Ractor.receive
  sum = 0
  pend = 0
  cp = nil
  t.times do |i|
    tag, h, cport = my.receive
    raise "tag" unless tag == :item
    raise "seq" unless h[:seq] == i
    raise "payload" unless h[:data] == i * 7
    sum += h[:data]
    cp = cport
    pend += 1
    if pend == batch
      b.send([:ack_batch, batch, cp])
      pend = 0
    end
  end
  raise "pend" unless pend == 0
  dp << [:cdone, sum]
end
buffer.send([:consumer, reg.receive])
cons.send(buffer)

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
    b.send([:item, { seq: seq, data: seq * 7 }, credit])
    credits -= 1
  end
  (t - got).times { raise "drain" unless credit.receive == :credit }
  dp << [:pdone]
end

sum = nil
2.times do
  m = done.receive
  sum = m[1] if m[0] == :cdone
end
raise "sum" unless sum == (0...T).sum { |i| i * 7 }
GC.stress = false
max_if = buffer.value
raise "cap" unless max_if <= C
cons.value; prod.value
puts "OK c25_pc_batch_acks"
