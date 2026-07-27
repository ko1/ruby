# c59: pipeline rebuilt per batch: for each batch a fresh 2-stage pipeline is
# spawned, run to completion, and value-joined (per-phase lifecycle).
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false

BATCHES = STRESS ? 2 : 4
T = STRESS ? 5 : 12
C = 2

BATCHES.times do |bi|
  GC.stress = true if STRESS && bi == 0
  sink = Ractor::Port.new
  s2 = Ractor.new(sink, T, bi) do |out, t, b|
    GC.stress = true if ENV['S_STRESS'] && b > 0
    t.times do
      tag, v, up = Ractor.receive
      raise "s2 tag" unless tag == :item
      out << [:item, v * 10 + b]
      up.send([:ack_down])
    end
    GC.stress = false
    :s2_done
  end
  s1 = Ractor.new(s2, T, C) do |down, t, cap|
    credits = cap
    q = []
    acked = 0
    fwd = lambda do |m|
      down.send([:item, m[1] + 100, Ractor.current])
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
          raise "s1 q" if q.size > cap
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
    raise "b#{bi} order" unless tag == :item && v == (i + 100) * 10 + bi
  end
  done.receive
  GC.stress = false
  raise unless s1.value == :s1_done && s2.value == :s2_done
  prod.value
  GC.start if bi == 0 && !STRESS
end
puts "OK c59_pipe_batch_respawn"
