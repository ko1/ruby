# c38: token bucket where grants carry moved token objects (ivar-bearing) with
# globally unique serials; main asserts the full serial set.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

B = 2
M = STRESS ? 3 : 4
R = STRESS ? 3 : 6
TOTAL = M * R

class Token
  attr_reader :serial, :window, :note
  def initialize(s, w)
    @serial = s
    @window = w
    @note = "tok-#{s}"
  end
end

driver = Ractor.new do
  lim = Ractor.receive
  loop do
    msg = Ractor.receive
    break if msg[0] == :stop
    lim.send([:tick])
  end
  :drv_done
end

limiter = Ractor.new(B, TOTAL, driver) do |cap, total, drv|
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
        tokens -= 1
        msg[1].send([:grant, Token.new(granted, window)], move: true)
        granted += 1
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
        tokens -= 1
        w.send([:grant, Token.new(granted, window)], move: true)
        granted += 1
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
    my = Ractor::Port.new
    serials = []
    last_w = -1
    r.times do
      lim.send([:req, my])
      tag, tok = my.receive
      raise "grant" unless tag == :grant
      raise "note" unless tok.note == "tok-#{tok.serial}"
      raise "monotone" unless tok.window >= last_w
      last_w = tok.window
      serials << tok.serial
    end
    dp.send([:done, id, serials], move: true)
  end
end

all = []
M.times do
  t, _, serials = done.receive
  raise "done" unless t == :done
  all.concat(serials)
end
raise "serials" unless all.sort == (0...TOTAL).to_a
GC.stress = false
raise unless limiter.value == TOTAL
driver.value
clients.each(&:value)
puts "OK c38_token_move_tokens"
