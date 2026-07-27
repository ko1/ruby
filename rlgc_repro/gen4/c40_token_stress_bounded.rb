# c40: token bucket with GC.stress bounded to client request loops; limiter runs
# GC.start every few messages and one bounded GC.compact.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false

B = 3
M = STRESS ? 3 : 4
R = STRESS ? 4 : 8
TOTAL = M * R

driver = Ractor.new do
  lim = Ractor.receive
  loop do
    msg = Ractor.receive
    break if msg[0] == :stop
    lim.send([:tick])
  end
  :drv_done
end

limiter = Ractor.new(B, TOTAL, driver, STRESS) do |cap, total, drv, st|
  tokens = cap
  window = 0
  waiting = []
  granted = 0
  tick_requested = false
  msgs = 0
  compacted = false
  while granted < total
    msg = Ractor.receive
    msgs += 1
    GC.start if msgs % 9 == 0 && msgs <= 27
    if msgs == 12 && !compacted && !st
      GC.compact
      compacted = true
    end
    case msg[0]
    when :req
      if tokens > 0 && !tick_requested
        tokens -= 1; granted += 1
        msg[1] << [:grant, window]
      else
        waiting << msg[1]
        unless tick_requested
          drv.send([:need_tick]); tick_requested = true
        end
      end
    when :tick
      window += 1
      tokens = cap
      tick_requested = false
      while tokens > 0 && (w = waiting.shift)
        tokens -= 1; granted += 1
        w << [:grant, window]
      end
      if !waiting.empty?
        drv.send([:need_tick]); tick_requested = true
      end
    end
  end
  drv.send([:stop])
  granted
end
driver.send(limiter)

done = Ractor::Port.new
clients = M.times.map do |i|
  Ractor.new(limiter, done, i, R) do |lim, dp, id, r|
    GC.stress = true if ENV['S_STRESS']
    my = Ractor::Port.new
    last_w = -1
    r.times do
      lim.send([:req, my])
      tag, w = my.receive
      raise "grant" unless tag == :grant
      raise "monotone" unless w >= last_w
      last_w = w
    end
    GC.stress = false
    dp << [:done, id]
  end
end

M.times { t, = done.receive; raise "done" unless t == :done }
raise unless limiter.value == TOTAL
driver.value
clients.each(&:value)
puts "OK c40_token_stress_bounded"
