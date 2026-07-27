# c24: bounded buffer where items are ivar-bearing objects moved along the whole
# chain producer->buffer->consumer->main; consumer verifies ivars per seq.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

C = 2
T = STRESS ? 6 : 25

class Item
  attr_reader :seq, :data, :blob
  def initialize(seq)
    @seq = seq
    @data = "data-#{seq}"
    @blob = [seq, seq * seq]
  end
end

buffer = Ractor.new(T, C) do |t, cap|
  tag, cport = Ractor.receive
  raise "reg" unless tag == :consumer
  in_flight = 0
  items = 0
  acks = 0
  until items == t && acks == t
    msg = Ractor.receive
    case msg[0]
    when :item
      in_flight += 1
      items += 1
      raise "overflow" if in_flight > cap
      cport.send([:item, msg[1], msg[2]], move: true)
    when :ack
      acks += 1
      in_flight -= 1
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
  digest = []
  t.times do |i|
    tag, it, cp = my.receive
    raise "tag" unless tag == :item
    raise "seq #{it.seq} != #{i}" unless it.seq == i
    raise "data" unless it.data == "data-#{i}" && it.blob == [i, i * i]
    digest << it.blob[1]
    b.send([:ack, cp])
  end
  dp.send([:cdone, digest], move: true)
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
    b.send([:item, Item.new(seq), credit], move: true)
    credits -= 1
  end
  (t - got).times { raise "drain" unless credit.receive == :credit }
  dp << [:pdone]
end

digest = nil
2.times do
  m = done.receive
  digest = m[1] if m[0] == :cdone
end
raise "digest" unless digest == T.times.map { |i| i * i }
GC.stress = false
raise unless buffer.value == :bdone
cons.value; prod.value
puts "OK c24_pc_move_items"
