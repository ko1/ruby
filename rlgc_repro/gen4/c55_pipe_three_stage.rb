# c55: 3-stage credit pipeline producer->s1->s2->sink(main); each link has its
# own credit loop; end-to-end FIFO and transform composition asserted.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

T = STRESS ? 8 : 30
C = 2

sink = Ractor::Port.new
s2 = Ractor.new(sink, T, C) do |out, t, cap|
  credits = cap
  q = []
  sent = 0
  acked = 0
  until acked == t
    msg = Ractor.receive
    case msg[0]
    when :item
      if credits > 0
        credits -= 1
        out << [:item, msg[1] + 5]
        sent += 1
        msg[2].send([:ack_down])
      else
        q << msg
        raise "s2 q" if q.size > cap
      end
    when :ack_down
      acked += 1
      credits += 1
      if (m = q.shift)
        credits -= 1
        out << [:item, m[1] + 5]
        sent += 1
        m[2].send([:ack_down])
      end
    end
  end
  raise "s2 end" unless q.empty? && sent == t
  :s2_done
end

s1 = Ractor.new(s2, T, C) do |down, t, cap|
  credits = cap
  q = []
  sent = 0
  acked = 0
  until acked == t
    msg = Ractor.receive
    case msg[0]
    when :item
      if credits > 0
        credits -= 1
        down.send([:item, msg[1] * 2, Ractor.current])
        sent += 1
        msg[2] << :credit
      else
        q << msg
        raise "s1 q" if q.size > cap
      end
    when :ack_down
      acked += 1
      credits += 1
      if (m = q.shift)
        credits -= 1
        down.send([:item, m[1] * 2, Ractor.current])
        sent += 1
        m[2] << :credit
      end
    end
  end
  raise "s1 end" unless q.empty? && sent == t
  :s1_done
end

done = Ractor::Port.new
prod = Ractor.new(s1, done, T, C) do |down, dp, t, cap|
  credit = Ractor::Port.new
  credits = cap
  got = 0
  t.times do |seq|
    if credits == 0
      raise "credit" unless credit.receive == :credit
      credits += 1
      got += 1
    end
    down.send([:item, seq, credit])
    credits -= 1
  end
  (t - got).times { raise "drain" unless credit.receive == :credit }
  dp << [:pdone]
end

T.times do |i|
  tag, v = sink.receive
  raise "sink" unless tag == :item
  raise "value #{v}" unless v == i * 2 + 5
  s2.send([:ack_down])
end
done.receive
GC.stress = false
raise unless s1.value == :s1_done && s2.value == :s2_done
prod.value
puts "OK c55_pipe_three_stage"
