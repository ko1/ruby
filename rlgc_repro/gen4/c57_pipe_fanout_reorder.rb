# c57: pipeline with parallel middle stage: s1 fans out by seq parity to s2a/s2b,
# s3 reorders by seq before the sink; reorder buffer bound asserted.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

T = STRESS ? 8 : 24   # even
CB = 2                # per-branch credit

sink = Ractor::Port.new
s3 = Ractor.new(sink, T, 2 * CB) do |out, t, maxbuf|
  buf = {}
  nxt = 0
  senders = {}
  while nxt < t
    tag, seq, from = Ractor.receive
    raise "s3 tag" unless tag == :item
    buf[seq] = true
    senders[seq] = from
    raise "reorder overflow #{buf.size}" if buf.size > maxbuf
    while buf.delete(nxt)
      out << [:item, nxt]
      senders.delete(nxt).send([:ack_down])
      nxt += 1
    end
  end
  raise "s3 end" unless buf.empty?
  :s3_done
end

mkbranch = lambda do |name, per|
  Ractor.new(s3, per, CB, name) do |down, t, cap, nm|
    credits = cap
    q = []
    acked = 0
    fwd = lambda do |m|
      down.send([:item, m[1], Ractor.current])
      m[2] << :credit
    end
    until acked == t
      msg = Ractor.receive
      case msg[0]
      when :item
        if credits > 0
          credits -= 1
          fwd.call(msg)
        else
          q << msg
          raise "#{nm} q" if q.size > cap
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
    nm
  end
end
s2a = mkbranch.call(:s2a, T / 2)
s2b = mkbranch.call(:s2b, T / 2)

done = Ractor::Port.new
prod = Ractor.new(s2a, s2b, done, T) do |a, b, dp, t|
  credit = Ractor::Port.new
  credits_a = 2
  credits_b = 2
  got = 0
  waits = 0
  t.times do |seq|
    branch = seq.even? ? a : b
    if seq.even?
      if credits_a == 0
        raise "credit" unless credit.receive == :credit
        got += 1
        credits_a += 1  # NOTE: credits return unlabeled; see below
      end
    else
      if credits_b == 0
        raise "credit" unless credit.receive == :credit
        got += 1
        credits_b += 1
      end
    end
    branch.send([:item, seq, credit])
    seq.even? ? credits_a -= 1 : credits_b -= 1
  end
  (t - got).times { raise "drain" unless credit.receive == :credit }
  dp << [:pdone, waits]
end

T.times do |i|
  tag, v = sink.receive
  raise "order #{v} != #{i}" unless tag == :item && v == i
end
done.receive
GC.stress = false
raise unless s3.value == :s3_done && s2a.value == :s2a && s2b.value == :s2b
prod.value
puts "OK c57_pipe_fanout_reorder"
