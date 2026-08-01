# c35: clock-less token bucket: limiter refills B tokens per driver tick; ticks
# are demand-driven (no timing). Per-window grants <= B asserted in limiter.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

B = 3
M = STRESS ? 3 : 4
R = STRESS ? 3 : 9
TOTAL = M * R

driver = Ractor.new do
  lim = Ractor.receive
  ticks = 0
  loop do
    msg = Ractor.receive
    break if msg[0] == :stop
    raise "drv" unless msg[0] == :need_tick
    ticks += 1
    lim.send([:tick])
  end
  ticks
end

limiter = Ractor.new(B, TOTAL, driver) do |cap, total, drv|
  tokens = cap
  window = 0
  waiting = []
  granted = 0
  per_window = 0
  tick_requested = false
  while granted < total
    msg = Ractor.receive
    case msg[0]
    when :req
      if tokens > 0 && !tick_requested
        tokens -= 1; granted += 1; per_window += 1
        raise "window overflow" if per_window > cap
        msg[1] << [:grant, window]
      else
        waiting << msg[1]
        unless tick_requested
          drv.send([:need_tick])
          tick_requested = true
        end
      end
    when :tick
      window += 1
      tokens = cap
      per_window = 0
      tick_requested = false
      while tokens > 0 && (w = waiting.shift)
        tokens -= 1; granted += 1; per_window += 1
        w << [:grant, window]
      end
      if !waiting.empty?
        drv.send([:need_tick])
        tick_requested = true
      end
    end
  end
  drv.send([:stop])
  [window, granted]
end
driver.send(limiter)

done = Ractor::Port.new
clients = M.times.map do |i|
  Ractor.new(limiter, done, i, R) do |lim, dp, id, r|
    my = Ractor::Port.new
    wins = []
    r.times do
      lim.send([:req, my])
      tag, w = my.receive
      raise "grant" unless tag == :grant
      wins << w
    end
    raise "monotone #{wins.inspect}" unless wins.each_cons(2).all? { |a, b| a <= b }
    dp << [:done, id, wins.size]
  end
end

grants = 0
M.times do
  t, _, n = done.receive
  raise "done" unless t == :done
  grants += n
end
raise "grants" unless grants == TOTAL
GC.stress = false
window, granted = limiter.value
raise "granted" unless granted == TOTAL
raise "windows #{window}" unless window + 1 >= (TOTAL + B - 1) / B
driver.value
clients.each(&:value)
puts "OK c35_token_bucket_basic"
