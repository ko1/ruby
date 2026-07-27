# two-stage routing: top router picks region by key[0], region router picks worker by key[1]
# axes: hierarchical dispatch (2 regions x 2 workers), :stop cascades through both stages
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

reg = Ractor::Port.new
done = Ractor::Port.new
workers = 4.times.map do |w|
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
wports = Array.new(4)
4.times do
  wid, p = reg.receive
  wports[wid] = p
end
regions = 2.times.map do |r|
  Ractor.new(r, wports[r * 2], wports[r * 2 + 1]) do |rid, w0, w1|
    loop do
      m = Ractor.receive
      if m == :stop
        w0.send(:stop)
        w1.send(:stop)
        break
      end
      sub, val = m
      (sub == 0 ? w0 : w1).send(val)
    end
    :fin
  end
end
top = Ractor.new(regions[0], regions[1]) do |r0, r1|
  loop do
    m = Ractor.receive
    if m == :stop
      r0.send(:stop)
      r1.send(:stop)
      break
    end
    region, sub, val = m
    (region == 0 ? r0 : r1).send([sub, val])
  end
  :fin
end
sums = Array.new(4, 0)
16.times do |k|
  region = k % 2
  sub = (k / 2) % 2
  sums[region * 2 + sub] += k + 5
  top.send([region, sub, k + 5])
end
top.send(:stop)
4.times do
  wid, sum = done.receive
  raise "worker #{wid}: #{sum} != #{sums[wid]}" unless sum == sums[wid]
end
GC.stress = false
raise unless top.value == :fin
regions.each { |r| raise unless r.value == :fin }
workers.each { |r| raise unless r.value == :fin }
puts "OK e48_route_twostage"
