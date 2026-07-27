# c60: expanding pipeline: s1 splits each input into 2 sub-items; s2 relays;
# sink sees 2T items in strict seq order; credit accounting spans the expansion.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

T = STRESS ? 5 : 15
C2 = 3               # s1->s2 link capacity (sub-item units)

sink = Ractor::Port.new
s2 = Ractor.new(sink, 2 * T) do |out, n|
  n.times do
    tag, v, up = Ractor.receive
    raise "s2 tag" unless tag == :item
    out << [:item, v]
    up.send([:ack_down])
  end
  :s2_done
end

s1 = Ractor.new(s2, T, C2) do |down, t, cap|
  credits = cap
  q = []            # queued sub-items [subseq, credit_port_or_nil]
  acked = 0
  sent = 0
  until acked == 2 * t
    msg = Ractor.receive
    case msg[0]
    when :item
      seq = msg[1]
      [seq * 2, seq * 2 + 1].each_with_index do |sub, k|
        cp = k == 1 ? msg[2] : nil     # upstream credited after 2nd sub queued/sent
        if credits > 0
          credits -= 1
          down.send([:item, sub, Ractor.current])
          sent += 1
          cp << :credit if cp
        else
          q << [sub, cp]
          raise "s1 q" if q.size > 2 * cap
        end
      end
    when :ack_down
      acked += 1
      credits += 1
      if (m = q.shift)
        credits -= 1
        down.send([:item, m[0], Ractor.current])
        sent += 1
        m[1] << :credit if m[1]
      end
    end
  end
  raise "s1 end" unless q.empty? && sent == 2 * t
  :s1_done
end

done = Ractor::Port.new
prod = Ractor.new(s1, done, T, 2) do |down, dp, t, cap|
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

(2 * T).times do |i|
  tag, v = sink.receive
  raise "order #{v} != #{i}" unless tag == :item && v == i
end
done.receive
GC.stress = false
raise unless s1.value == :s1_done && s2.value == :s2_done
prod.value
GC.start
puts "OK c60_pipe_expand"
