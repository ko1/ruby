# c22: bounded buffer with P producers (per-producer credits C=2), one consumer;
# consumer asserts per-producer FIFO; string payloads.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

P = STRESS ? 3 : 4
C = 2
T = STRESS ? 4 : 15   # items per producer

buffer = Ractor.new(P, T, C) do |np, t, cap|
  tag, cport = Ractor.receive
  raise "reg" unless tag == :consumer
  in_flight = Array.new(np, 0)
  items = 0
  acks = 0
  until items == np * t && acks == np * t
    msg = Ractor.receive
    case msg[0]
    when :item
      pid = msg[1]
      in_flight[pid] += 1
      items += 1
      raise "overflow p#{pid}" if in_flight[pid] > cap
      cport << [:item, pid, msg[2], msg[3], msg[4]]
    when :ack
      acks += 1
      in_flight[msg[1]] -= 1
      raise "underflow" if in_flight[msg[1]] < 0
      msg[2] << :credit
    end
  end
  raise "end" unless in_flight.all?(&:zero?)
  [items, acks]
end

reg = Ractor::Port.new
done = Ractor::Port.new
cons = Ractor.new(reg, done, P, T) do |rp, dp, np, t|
  my = Ractor::Port.new
  rp << my
  b = Ractor.receive
  nexts = Array.new(np, 0)
  sum = 0
  (np * t).times do
    tag, pid, seq, body, cp = my.receive
    raise "tag" unless tag == :item
    raise "fifo p#{pid}: #{seq} != #{nexts[pid]}" unless seq == nexts[pid]
    raise "body" unless body == "p#{pid}-s#{seq}"
    nexts[pid] += 1
    sum += seq
    b.send([:ack, pid, cp])
  end
  dp << [:cdone, sum]
end
buffer.send([:consumer, reg.receive])
cons.send(buffer)

prods = P.times.map do |pid|
  Ractor.new(buffer, done, pid, T, C) do |b, dp, id, t, cap|
    credit = Ractor::Port.new
    credits = cap
    got = 0
    t.times do |seq|
      if credits == 0
        raise "credit" unless credit.receive == :credit
        credits += 1
        got += 1
      end
      b.send([:item, id, seq, "p#{id}-s#{seq}", credit])
      credits -= 1
    end
    (t - got).times { raise "drain" unless credit.receive == :credit }
    dp << [:pdone, id]
  end
end

csum = nil
pdones = 0
(P + 1).times do
  tag, v = done.receive
  if tag == :cdone then csum = v else raise "pd" unless tag == :pdone; pdones += 1 end
end
raise "pdones" unless pdones == P
raise "csum" unless csum == P * (0...T).sum
GC.stress = false
items, acks = buffer.value
raise "counts" unless items == P * T && acks == P * T
cons.value; prods.each(&:value)
puts "OK c22_pc_multi_producers"
