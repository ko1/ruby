# c58: 3-stage pipeline with different per-link capacities (1,2,3); every stage
# asserts its own queue ceiling; sink asserts FIFO.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

T = STRESS ? 8 : 28
CAPS = [1, 2, 3].freeze

def mkstage(down, t, cap, name, port_down)
  Ractor.new(down, t, cap, name, port_down) do |dn, tt, cp, nm, pd|
    credits = cp
    q = []
    maxq = 0
    acked = 0
    fwd = lambda do |m|
      v = m[1] + 1
      if pd
        dn << [:item, v]
      else
        dn.send([:item, v, Ractor.current])
      end
      if m[2].is_a?(Ractor::Port)
        m[2] << :credit
      else
        m[2].send([:ack_down])   # upstream is a stage ractor
      end
    end
    until acked == tt
      msg = Ractor.receive
      case msg[0]
      when :item
        if credits > 0
          credits -= 1
          fwd.call(msg)
        else
          q << msg
          maxq = q.size if q.size > maxq
          raise "#{nm} q #{q.size}" if q.size > cp
        end
      when :ack_down
        acked += 1
        credits += 1
        if (m = q.shift)
          credits -= 1
          fwd.call(m)
        end
      end
    end
    raise "#{nm} end" unless q.empty?
    [nm, maxq]
  end
end

sink = Ractor::Port.new
s3 = mkstage(sink, T, CAPS[2], :s3, true)
s2 = mkstage(s3, T, CAPS[1], :s2, false)
s1 = mkstage(s2, T, CAPS[0], :s1, false)

done = Ractor::Port.new
prod = Ractor.new(s1, done, T, CAPS[0]) do |down, dp, t, cap|
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
  raise "order" unless tag == :item && v == i + 3
  s3.send([:ack_down])
end
done.receive
GC.stress = false
n1, q1 = s1.value
n2, q2 = s2.value
n3, q3 = s3.value
raise "names" unless n1 == :s1 && n2 == :s2 && n3 == :s3
raise "caps" unless q1 <= CAPS[0] && q2 <= CAPS[1] && q3 <= CAPS[2]
prod.value
puts "OK c58_pipe_uneven_caps"
