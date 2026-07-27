# c01: counting semaphore ractor (K permits, M clients, R cycles); copy payloads.
# GC axes: GC.stress in main under S_STRESS; final join after done-ports.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

K = 2
M = STRESS ? 3 : 6
R = STRESS ? 4 : 20

done_port = Ractor::Port.new

sema = Ractor.new(K) do |k|
  active = 0
  waiting = []
  grants = 0
  loop do
    msg = Ractor.receive
    case msg[0]
    when :acquire
      if active < k
        active += 1
        grants += 1
        msg[1] << :grant
      else
        waiting << msg[1]
      end
    when :release
      raise "release underflow" if active <= 0
      if (w = waiting.shift)
        grants += 1
        w << :grant
      else
        active -= 1
      end
    when :stop
      raise "active at stop: #{active}" unless active == 0
      raise "waiting at stop" unless waiting.empty?
      break grants
    end
    raise "invariant active>k" if active > k
  end
end

clients = M.times.map do |i|
  Ractor.new(sema, done_port, i, R) do |s, dp, id, rounds|
    reply = Ractor::Port.new
    acc = 0
    rounds.times do |t|
      s.send([:acquire, reply])
      g = reply.receive
      raise "bad grant" unless g == :grant
      acc += id * 1000 + t
      s.send([:release])
    end
    dp << [:done, id, acc]
  end
end

sum = 0
seen = {}
M.times do
  tag, id, acc = done_port.receive
  raise "bad tag" unless tag == :done
  raise "dup id" if seen[id]
  seen[id] = true
  sum += acc
end
expected = M.times.sum { |i| R.times.sum { |t| i * 1000 + t } }
raise "sum #{sum} != #{expected}" unless sum == expected

sema.send([:stop])
GC.stress = false
grants = sema.value
raise "grants #{grants} != #{M * R}" unless grants == M * R
clients.each(&:value)
GC.start
puts "OK c01_sema_basic"
