# c37: token bucket, single client fires a full burst of N pipelined requests;
# grants split across windows: per-window <= B, windows non-decreasing, >= ceil(N/B).
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

B = 3
N = STRESS ? 8 : 24

driver = Ractor.new do
  lim = Ractor.receive
  loop do
    msg = Ractor.receive
    break if msg[0] == :stop
    lim.send([:tick])
  end
  :drv_done
end

limiter = Ractor.new(B, N, driver) do |cap, total, drv|
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
  window
end
driver.send(limiter)

done = Ractor::Port.new
client = Ractor.new(limiter, done, N) do |lim, dp, n|
  my = Ractor::Port.new
  n.times { lim.send([:req, my]) }          # full burst, no waiting between
  wins = n.times.map do
    tag, w = my.receive
    raise "grant" unless tag == :grant
    w
  end
  raise "monotone" unless wins.each_cons(2).all? { |a, b| a <= b }
  hist = Hash.new(0)
  wins.each { |w| hist[w] += 1 }
  hist.each { |_, c| raise "per-window #{c}" unless c <= 3 }
  dp << [:done, wins.size, hist.size]
end

tag, n, nwin = done.receive
raise "done" unless tag == :done && n == N
raise "windows #{nwin}" unless nwin >= (N + B - 1) / B
GC.stress = false
limiter.value
driver.value
client.value
GC.start
puts "OK c37_token_burst"
