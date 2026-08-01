# c02: counting semaphore guarding a pool of K mutable string resources.
# Resources moved to grantees and moved back on release; usage tallied via string growth.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

K = 2
M = STRESS ? 3 : 5
R = STRESS ? 3 : 12

done = Ractor::Port.new
sema = Ractor.new(K) do |k|
  pool = Array.new(k) { |i| "r#{i}:" }
  waiting = []
  grants = 0
  loop do
    msg = Ractor.receive
    case msg[0]
    when :acquire
      if pool.empty?
        waiting << msg[1]
      else
        res = pool.pop
        grants += 1
        msg[1].send(res, move: true)
      end
    when :release
      res = msg[1]
      if (w = waiting.shift)
        grants += 1
        w.send(res, move: true)
      else
        pool.push(res)
      end
    when :stop
      raise "pool short: #{pool.size}" unless pool.size == k
      raise "waiting at stop" unless waiting.empty?
      used = pool.sum { |s| s.count("u") }
      break [grants, used]
    end
  end
end

clients = M.times.map do |i|
  Ractor.new(sema, done, i, R) do |s, dp, id, rounds|
    reply = Ractor::Port.new
    rounds.times do
      s.send([:acquire, reply])
      res = reply.receive
      raise "bad res #{res}" unless res.start_with?("r")
      res << "u"
      s.send([:release, res], move: true)
    end
    dp << [:done, id]
  end
end

M.times { t, = done.receive; raise "bad done" unless t == :done }
sema.send([:stop])
GC.stress = false
grants, used = sema.value
raise "grants #{grants}" unless grants == M * R
raise "used #{used}" unless used == M * R
clients.each(&:value)
puts "OK c02_sema_move_pool"
