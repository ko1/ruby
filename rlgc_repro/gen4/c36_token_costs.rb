# c36: token bucket with weighted request costs and FIFO head-of-line queueing;
# per-window cost sum <= B asserted; total cost accounting checked.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

B = 4
M = STRESS ? 3 : 4
R = STRESS ? 3 : 8

driver = Ractor.new do
  lim = Ractor.receive
  loop do
    msg = Ractor.receive
    break if msg[0] == :stop
    lim.send([:tick])
  end
  :drv_done
end

total_reqs = M * R
limiter = Ractor.new(B, total_reqs, driver) do |cap, total, drv|
  tokens = cap
  window = 0
  waiting = []       # [cost, port]
  served = 0
  cost_sum = 0
  win_cost = 0
  tick_requested = false
  while served < total
    msg = Ractor.receive
    case msg[0]
    when :req
      waiting << [msg[1], msg[2]]
      while !tick_requested && !waiting.empty? && waiting.first[0] <= tokens
        c, port = waiting.shift
        tokens -= c
        win_cost += c
        raise "win overflow" if win_cost > cap
        served += 1
        cost_sum += c
        port << [:grant, window]
      end
      if !waiting.empty? && !tick_requested
        drv.send([:need_tick])
        tick_requested = true
      end
    when :tick
      window += 1
      tokens = cap
      win_cost = 0
      tick_requested = false
      while !waiting.empty? && waiting.first[0] <= tokens
        c, port = waiting.shift
        tokens -= c
        win_cost += c
        served += 1
        cost_sum += c
        port << [:grant, window]
      end
      if !waiting.empty?
        drv.send([:need_tick])
        tick_requested = true
      end
    end
  end
  drv.send([:stop])
  cost_sum
end
driver.send(limiter)

done = Ractor::Port.new
clients = M.times.map do |i|
  Ractor.new(limiter, done, i, R) do |lim, dp, id, r|
    my = Ractor::Port.new
    spent = 0
    last_w = -1
    r.times do |t|
      cost = (id + t) % 3 + 1    # 1..3, all <= B
      lim.send([:req, cost, my])
      tag, w = my.receive
      raise "grant" unless tag == :grant
      raise "monotone" unless w >= last_w
      last_w = w
      spent += cost
    end
    dp << [:done, id, spent]
  end
end

spent_total = 0
M.times do
  t, _, s = done.receive
  raise "done" unless t == :done
  spent_total += s
end
expected = M.times.sum { |i| R.times.sum { |t| (i + t) % 3 + 1 } }
raise "spent" unless spent_total == expected
GC.stress = false
raise "cost" unless limiter.value == expected
driver.value
clients.each(&:value)
puts "OK c36_token_costs"
