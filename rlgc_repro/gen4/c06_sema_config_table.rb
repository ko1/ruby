# c06: semaphore clients read a deeply-frozen shareable config table (weights);
# GC axis: GC.stress inside participants (bounded), GC.start scattered.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false

M = STRESS ? 3 : 6
R = STRESS ? 3 : 15
CONFIG = Ractor.make_shareable({ k: 2, weights: (1..8).map { |x| x * 7 }, tag: "cfg" })

sema = Ractor.new(CONFIG[:k]) do |k|
  active = 0
  waiting = []
  grants = 0
  loop do
    msg = Ractor.receive
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
    GC.stress = true if ENV['S_STRESS']
    reply = Ractor::Port.new
    acc = 0
    rounds.times do |t|
      s.send([:acquire, reply])
      raise "grant" unless reply.receive == :grant
      acc += CONFIG[:weights][id % CONFIG[:weights].size] * (t + 1)
      raise "tag" unless CONFIG[:tag] == "cfg"
      s.send([:release])
      GC.start if t == rounds / 2 && !ENV['S_STRESS']
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
w = CONFIG[:weights]
expected = M.times.sum { |i| R.times.sum { |t| w[i % w.size] * (t + 1) } }
raise "sum #{sum} != #{expected}" unless sum == expected
sema.send([:stop])
raise "grants" unless sema.value == M * R
clients.each(&:value)
puts "OK c06_sema_config_table"
