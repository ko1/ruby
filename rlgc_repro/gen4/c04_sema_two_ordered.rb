# c04: two semaphores (A: K=2, B: K=1) acquired in fixed order A->B (no deadlock);
# both track invariants; copy payloads; stress in main.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

M = STRESS ? 3 : 5
R = STRESS ? 3 : 10

def make_sema(k)
  Ractor.new(k) do |cap|
    active = 0
    waiting = []
    grants = 0
    loop do
      msg = Ractor.receive
      case msg[0]
      when :acquire
        if active < cap
          active += 1
          grants += 1
          msg[1] << :grant
        else
          waiting << msg[1]
        end
      when :release
        raise "underflow" if active <= 0
        if (w = waiting.shift)
          grants += 1
          w << :grant
        else
          active -= 1
        end
      when :stop
        raise "active #{active}" unless active == 0
        break grants
      end
      raise "over cap" if active > cap
    end
  end
end

sa = make_sema(2)
sb = make_sema(1)
done = Ractor::Port.new

clients = M.times.map do |i|
  Ractor.new(sa, sb, done, i, R) do |a, b, dp, id, rounds|
    reply = Ractor::Port.new
    acc = 0
    rounds.times do |t|
      a.send([:acquire, reply])
      raise "a" unless reply.receive == :grant
      b.send([:acquire, reply])
      raise "b" unless reply.receive == :grant
      acc += (id + 1) * (t + 1)
      b.send([:release])
      a.send([:release])
    end
    dp << [:done, id, acc]
  end
end

sum = 0
M.times do
  t, _, acc = done.receive
  raise "done" unless t == :done
  sum += acc
end
expected = M.times.sum { |i| R.times.sum { |t| (i + 1) * (t + 1) } }
raise "sum" unless sum == expected
sa.send([:stop]); sb.send([:stop])
GC.stress = false
raise "ga" unless sa.value == M * R
raise "gb" unless sb.value == M * R
clients.each(&:value)
puts "OK c04_sema_two_ordered"
