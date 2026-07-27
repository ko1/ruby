# c21: bounded buffer, 1 producer / 1 consumer, credit-based backpressure (C=3);
# buffer asserts in-flight <= C; consumer asserts FIFO seq order.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

C = 3
T = STRESS ? 8 : 40

buffer = Ractor.new(T, C) do |t, cap|
  tag, cport = Ractor.receive
  raise "reg" unless tag == :consumer
  in_flight = 0
  max_if = 0
  items = 0
  acks = 0
  until items == t && acks == t
    msg = Ractor.receive
    case msg[0]
    when :item
      in_flight += 1
      items += 1
      max_if = in_flight if in_flight > max_if
      raise "overflow #{in_flight}" if in_flight > cap
      cport << [:item, msg[1], msg[2]]
    when :ack
      acks += 1
      in_flight -= 1
      raise "underflow" if in_flight < 0
      msg[1] << :credit
    else
      raise "msg"
    end
  end
  raise "inflight end" unless in_flight == 0
  [max_if, items, acks]
end

reg = Ractor::Port.new
done = Ractor::Port.new
cons = Ractor.new(reg, done, T) do |rp, dp, t|
  my = Ractor::Port.new
  rp << my
  b = Ractor.receive
  sum = 0
  t.times do |i|
    tag, seq, cp = my.receive
    raise "order #{seq} != #{i}" unless tag == :item && seq == i
    sum += seq
    b.send([:ack, cp])
  end
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
    b.send([:item, seq, credit])
    credits -= 1
  end
  (t - got).times { raise "drain" unless credit.receive == :credit }
  dp << [:pdone, t]
end

got = {}
2.times do
  tag, v = done.receive
  got[tag] = v
end
raise "cdone" unless got[:cdone] == (0...T).sum
raise "pdone" unless got[:pdone] == T
GC.stress = false
max_if, items, acks = buffer.value
raise "cap #{max_if}" unless max_if <= C && max_if >= 1
raise "counts" unless items == T && acks == T
cons.value; prod.value
GC.start
puts "OK c21_pc_bounded_basic"
