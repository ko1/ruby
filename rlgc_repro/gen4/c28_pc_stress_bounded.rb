# c28: bounded buffer with GC.stress confined to producer/consumer hot loops and
# periodic bounded GC.compact inside the buffer ractor.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false

C = 3
T = STRESS ? 10 : 30

buffer = Ractor.new(T, C, STRESS) do |t, cap, st|
  tag, cport = Ractor.receive
  raise "reg" unless tag == :consumer
  in_flight = 0
  items = 0
  acks = 0
  compacts = 0
  until items == t && acks == t
    msg = Ractor.receive
    case msg[0]
    when :item
      in_flight += 1; items += 1
      raise "overflow" if in_flight > cap
      if items % 10 == 0 && compacts < 2 && !st
        GC.compact
        compacts += 1
      end
      cport << [:item, msg[1], msg[2]]
    when :ack
      acks += 1; in_flight -= 1
      msg[1] << :credit
    end
  end
  raise "end" unless in_flight == 0
  :bdone
end

reg = Ractor::Port.new
done = Ractor::Port.new
cons = Ractor.new(reg, done, T) do |rp, dp, t|
  my = Ractor::Port.new
  rp << my
  b = Ractor.receive
  sum = 0
  GC.stress = true if ENV['S_STRESS']
  t.times do |i|
    tag, arr, cp = my.receive
    raise "tag" unless tag == :item
    raise "seq" unless arr[0] == i
    sum += arr[1].sum
    b.send([:ack, cp])
  end
  GC.stress = false
  dp << [:cdone, sum]
end
buffer.send([:consumer, reg.receive])
cons.send(buffer)

prod = Ractor.new(buffer, done, T, C) do |b, dp, t, cap|
  credit = Ractor::Port.new
  credits = cap
  got = 0
  GC.stress = true if ENV['S_STRESS']
  t.times do |seq|
    if credits == 0
      raise "credit" unless credit.receive == :credit
      credits += 1
      got += 1
    end
    b.send([:item, [seq, [seq, seq + 1, seq + 2]], credit])
    credits -= 1
  end
  GC.stress = false
  (t - got).times { raise "drain" unless credit.receive == :credit }
  dp << [:pdone]
end

sum = nil
2.times do
  m = done.receive
  sum = m[1] if m[0] == :cdone
end
raise "sum" unless sum == (0...T).sum { |s| s + (s + 1) + (s + 2) }
raise unless buffer.value == :bdone
cons.value; prod.value
puts "OK c28_pc_stress_bounded"
