# least-loaded balancing: dispatcher tracks outstanding per worker via counted acks
# axes: ack-driven load tracking, invariant checks (outstanding==0 at end, totals match)
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

W = 3
J = 15
WINDOW = 2
reg = Ractor::Port.new
done = Ractor::Port.new
workers = W.times.map do |w|
  Ractor.new(w, reg, done) do |wid, regp, dport|
    inbox = Ractor::Port.new
    regp.send([wid, inbox])
    sum = 0
    cnt = 0
    loop do
      m = inbox.receive
      break if m == :stop
      ackp, v = m
      sum += v
      cnt += 1
      ackp.send(wid)
    end
    dport.send([wid, cnt, sum])
    :fin
  end
end
wports = Array.new(W)
W.times do
  wid, p = reg.receive
  wports[wid] = p
end
dispatcher = Ractor.new(wports, J, WINDOW) do |wp, jobs, window|
  ackp = Ractor::Port.new
  outstanding = Array.new(wp.size, 0)
  sent = 0
  acked = 0
  while sent < jobs
    while outstanding.sum >= window
      outstanding[ackp.receive] -= 1
      acked += 1
    end
    target = outstanding.index(outstanding.min)
    wp[target].send([ackp, sent + 1])
    outstanding[target] += 1
    sent += 1
  end
  while acked < jobs
    outstanding[ackp.receive] -= 1
    acked += 1
  end
  raise "outstanding" unless outstanding.all?(&:zero?)
  wp.each { |p| p.send(:stop) }
  :disp_fin
end
tot_cnt = 0
tot_sum = 0
W.times do
  _wid, cnt, sum = done.receive
  tot_cnt += cnt
  tot_sum += sum
end
raise "counts #{tot_cnt}" unless tot_cnt == J
raise "sums #{tot_sum}" unless tot_sum == (1..J).sum
GC.stress = false
raise unless dispatcher.value == :disp_fin
workers.each { |r| raise unless r.value == :fin }
puts "OK e60_lb_least"
