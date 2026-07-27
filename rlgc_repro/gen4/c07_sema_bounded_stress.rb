# c07: semaphore with GC.stress bounded to clients' critical loops only;
# semaphore ractor runs GC.compact periodically (bounded count).
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false

K = 3
M = STRESS ? 4 : 6
R = STRESS ? 3 : 10

sema = Ractor.new(K, STRESS) do |k, st|
  active = 0
  waiting = []
  grants = 0
  msgs = 0
  compacts = 0
  loop do
    msg = Ractor.receive
    msgs += 1
    if msgs % 11 == 0 && compacts < 3 && !st
      GC.compact
      compacts += 1
    end
    case msg[0]
    when :acquire
      if active < k
        active += 1; grants += 1; msg[1] << :grant
      else
        waiting << msg[1]
      end
    when :release
      raise "underflow" if active <= 0
      if (w = waiting.shift) then grants += 1; w << :grant else active -= 1 end
    when :stop
      raise "active" unless active == 0
      break grants
    end
    raise "cap" if active > k
  end
end

done = Ractor::Port.new
clients = M.times.map do |i|
  Ractor.new(sema, done, i, R) do |s, dp, id, rounds|
    reply = Ractor::Port.new
    acc = 0
    GC.stress = true if ENV['S_STRESS']   # bounded: cleared before exit
    rounds.times do |t|
      s.send([:acquire, reply])
      raise "grant" unless reply.receive == :grant
      buf = Array.new(8) { |x| x + id + t }
      acc += buf.sum
      s.send([:release])
    end
    GC.stress = false
    dp << [:done, id, acc]
  end
end

sum = 0
M.times do
  t, _, acc = done.receive
  raise "done" unless t == :done
  sum += acc
end
expected = M.times.sum { |i| R.times.sum { |t| Array.new(8) { |x| x + i + t }.sum } }
raise "sum" unless sum == expected
sema.send([:stop])
raise "grants" unless sema.value == M * R
clients.each(&:value)
GC.start
puts "OK c07_sema_bounded_stress"
