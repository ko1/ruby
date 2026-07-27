# c27: two bounded stages chained (producer->buf1(C1)->buf2(C2)->consumer); each
# stage has its own credit loop; end-to-end FIFO and both ceilings asserted.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

C1 = 2
C2 = 3
T = STRESS ? 6 : 24

buf2 = Ractor.new(T, C2) do |t, cap|
  tag2, b1 = Ractor.receive
  raise "reg b1" unless tag2 == :upstream
  tag, cport = Ractor.receive
  raise "reg c" unless tag == :consumer
  in_flight = 0
  max_if = 0
  items = 0
  acks = 0
  until items == t && acks == t
    msg = Ractor.receive
    case msg[0]
    when :item
      in_flight += 1; items += 1
      max_if = in_flight if in_flight > max_if
      raise "b2 overflow" if in_flight > cap
      cport << [:item, msg[1]]
    when :ack
      acks += 1; in_flight -= 1
      b1.send([:ack1])
    end
  end
  raise "b2 end" unless in_flight == 0
  max_if
end

buf1 = Ractor.new(buf2, T, C1, C2) do |b2, t, c1, c2|
  q = []
  max_q = 0
  credits2 = c2
  items = 0
  acks1 = 0
  until items == t && acks1 == t
    msg = Ractor.receive
    case msg[0]
    when :item
      items += 1
      if credits2 > 0
        credits2 -= 1
        b2.send([:item, msg[1]])
        msg[2] << :credit
      else
        q << msg
        max_q = q.size if q.size > max_q
        raise "b1 overflow" if q.size > c1
      end
    when :ack1
      acks1 += 1
      credits2 += 1
      if (m = q.shift)
        credits2 -= 1
        b2.send([:item, m[1]])
        m[2] << :credit
      end
    end
  end
  raise "b1 q end" unless q.empty?
  max_q
end
buf2.send([:upstream, buf1])

reg = Ractor::Port.new
done = Ractor::Port.new
cons = Ractor.new(reg, done, T) do |rp, dp, t|
  my = Ractor::Port.new
  rp << my
  b2 = Ractor.receive
  t.times do |i|
    tag, seq = my.receive
    raise "order #{seq} != #{i}" unless tag == :item && seq == i
    b2.send([:ack])
  end
  dp << [:cdone, t]
end
buf2.send([:consumer, reg.receive])
cons.send(buf2)

prod = Ractor.new(buf1, done, T, C1) do |b1, dp, t, cap|
  credit = Ractor::Port.new
  credits = cap
  got = 0
  t.times do |seq|
    if credits == 0
      raise "credit" unless credit.receive == :credit
      credits += 1
      got += 1
    end
    b1.send([:item, seq, credit])
    credits -= 1
  end
  (t - got).times { raise "drain" unless credit.receive == :credit }
  dp << [:pdone]
end

2.times { done.receive }
GC.stress = false
max_q = buf1.value
max_if = buf2.value
raise "b1 cap" unless max_q <= C1
raise "b2 cap" unless max_if <= C2 && max_if >= 1
cons.value; prod.value
GC.start
puts "OK c27_pc_two_stage"
