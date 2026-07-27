# c26: bounded producer/consumer where buffer, producer and consumer are all
# respawned per phase (value-chain teardown); GC.compact between phases.
# Stress axis: main GC.stress in phase 0 only; participant-local stress afterwards
# (CHECK_MODE=2 build makes whole-run main stress on a grown heap too slow).
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false

PHASES = STRESS ? 2 : 4
C = 2
T = STRESS ? 4 : 12

PHASES.times do |ph|
  GC.stress = true if STRESS && ph == 0
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
        in_flight += 1; items += 1
        raise "overflow" if in_flight > cap
        cport << [:item, msg[1], msg[2]]
      when :ack
        acks += 1; in_flight -= 1
        msg[1] << :credit
      end
    end
    raise "end" unless in_flight == 0
    items
  end

  reg = Ractor::Port.new
  done = Ractor::Port.new
  cons = Ractor.new(reg, done, T, ph) do |rp, dp, t, phase|
    GC.stress = true if ENV['S_STRESS'] && phase > 0
    my = Ractor::Port.new
    rp << my
    b = Ractor.receive
    sum = 0
    t.times do |i|
      tag, v, cp = my.receive
      raise "tag" unless tag == :item
      raise "val" unless v == phase * 1000 + i
      sum += v
      b.send([:ack, cp])
    end
    GC.stress = false
    dp << [:cdone, sum]
  end
  buffer.send([:consumer, reg.receive])
  cons.send(buffer)

  prod = Ractor.new(buffer, done, T, C, ph) do |b, dp, t, cap, phase|
    GC.stress = true if ENV['S_STRESS'] && phase > 0
    credit = Ractor::Port.new
    credits = cap
    got = 0
    t.times do |seq|
      if credits == 0
        raise "credit" unless credit.receive == :credit
        credits += 1
        got += 1
      end
      b.send([:item, phase * 1000 + seq, credit])
      credits -= 1
    end
    (t - got).times { raise "drain" unless credit.receive == :credit }
    GC.stress = false
    dp << [:pdone]
  end

  sum = nil
  2.times do
    m = done.receive
    sum = m[1] if m[0] == :cdone
  end
  raise "sum ph#{ph}" unless sum == T.times.sum { |i| ph * 1000 + i }
  GC.stress = false
  raise unless buffer.value == T
  cons.value; prod.value
  GC.compact if ph == 1 && !STRESS
  GC.start if ph == 0 && !STRESS
end
puts "OK c26_pc_respawn_phases"
