# c56: 4-stage pipeline moving mutable payloads end to end; each stage appends
# its tag to the moved array; sink asserts the full tag chain per seq.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

T = STRESS ? 6 : 20
C = 2

def stage(downstream, t, cap, tag, is_port: false)
  Ractor.new(downstream, t, cap, tag, is_port) do |down, tt, cp, tg, port|
    credits = cp
    q = []
    acked = 0
    fwd = lambda do |m|
      arr = m[1]
      arr << tg
      if port
        down.send([:item, arr], move: true)
      else
        down.send([:item, arr, Ractor.current], move: true)
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
          raise "#{tg} q" if q.size > cp
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
    raise "#{tg} end" unless q.empty?
    tg
  end
end

sink = Ractor::Port.new
s3 = stage(sink, T, C, :s3, is_port: true)
s2 = stage(s3, T, C, :s2)
s1 = stage(s2, T, C, :s1)

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
    down.send([:item, [seq], credit], move: true)
    credits -= 1
  end
  (t - got).times { raise "drain" unless credit.receive == :credit }
  dp << [:pdone]
end

T.times do |i|
  tag, arr = sink.receive
  raise "sink" unless tag == :item
  raise "chain #{arr.inspect}" unless arr == [i, :s1, :s2, :s3]
  s3.send([:ack_down])
end
done.receive
GC.stress = false
raise unless s1.value == :s1 && s2.value == :s2 && s3.value == :s3
prod.value
puts "OK c56_pipe_move_chain"
