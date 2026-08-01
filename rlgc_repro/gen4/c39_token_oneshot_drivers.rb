# c39: token bucket where each tick is produced by a fresh one-shot driver ractor
# spawned by main on demand (ractor churn in the grant path).
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

B = 2
M = STRESS ? 3 : 4
R = STRESS ? 2 : 5
TOTAL = M * R

need = Ractor::Port.new
limiter = Ractor.new(B, TOTAL, need) do |cap, total, needp|
  tokens = cap
  window = 0
  waiting = []
  granted = 0
  tick_requested = false
  while granted < total
    msg = Ractor.receive
    case msg[0]
    when :req
      if tokens > 0 && !tick_requested
        tokens -= 1; granted += 1
        msg[1] << [:grant, window]
      else
        waiting << msg[1]
        unless tick_requested
          needp << [:need_tick]
          tick_requested = true
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
        needp << [:need_tick]
        tick_requested = true
      end
    end
  end
  needp << [:all_granted]
  [window, granted]
end

done = Ractor::Port.new
clients = M.times.map do |i|
  Ractor.new(limiter, done, i, R) do |lim, dp, id, r|
    my = Ractor::Port.new
    last_w = -1
    r.times do
      lim.send([:req, my])
      tag, w = my.receive
      raise "grant" unless tag == :grant
      raise "monotone" unless w >= last_w
      last_w = w
    end
    dp << [:done, id]
  end
end

drivers = []
dones = 0
finished = false
until finished && dones == M
  msg = need.receive
  case msg[0]
  when :need_tick
    drivers << Ractor.new(limiter) { |lim| lim.send([:tick]); :tick_sent }
  when :all_granted
    finished = true
    M.times do
      t, = done.receive
      raise "done" unless t == :done
      dones += 1
    end
  else
    raise "need msg #{msg.inspect}"
  end
end
GC.stress = false
window, granted = limiter.value
raise "granted" unless granted == TOTAL
raise "windows" unless window + 1 >= (TOTAL + B - 1) / B
raise "drivers" unless drivers.size == window
drivers.each { |d| raise "drv" unless d.value == :tick_sent }
clients.each(&:value)
puts "OK c39_token_oneshot_drivers"
