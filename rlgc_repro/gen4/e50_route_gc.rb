# router with GC: GC.start every 5 routed messages, GC.compact at router before :stop fanout
# axes: GC pressure at the routing hot spot, 2 workers
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

reg = Ractor::Port.new
done = Ractor::Port.new
workers = 2.times.map do |w|
  Ractor.new(w, reg, done) do |wid, regp, dport|
    inbox = Ractor::Port.new
    regp.send([wid, inbox])
    sum = 0
    loop do
      m = inbox.receive
      break if m == :stop
      sum += m
    end
    dport.send([wid, sum])
    :fin
  end
end
wports = Array.new(2)
2.times do
  wid, p = reg.receive
  wports[wid] = p
end
router = Ractor.new(wports) do |wp|
  n = 0
  loop do
    m = Ractor.receive
    if m == :stop
      GC.compact
      wp.each { |p| p.send(:stop) }
      break
    end
    wp[m % 2].send(m)
    n += 1
    GC.start if n % 5 == 0
  end
  :fin
end
vals = (1..20).to_a
vals.each { |v| router.send(v) }
router.send(:stop)
2.times do
  wid, sum = done.receive
  exp = vals.select { |v| v % 2 == wid }.sum
  raise "worker #{wid}" unless sum == exp
end
GC.stress = false
raise unless router.value == :fin
workers.each { |r| raise unless r.value == :fin }
puts "OK e50_route_gc"
