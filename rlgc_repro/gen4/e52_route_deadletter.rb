# routing with dead-letter queue: unknown keys go to a dead-letter worker which records them
# axes: fallback route, mixed known/unknown stream, GC.start in dead-letter worker
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

reg = Ractor::Port.new
done = Ractor::Port.new
workers = 2.times.map do |w|
  Ractor.new(w, reg, done) do |wid, regp, dport|
    inbox = Ractor::Port.new
    regp.send([wid, inbox])
    cnt = 0
    loop do
      m = inbox.receive
      break if m == :stop
      cnt += 1
    end
    dport.send([wid, cnt])
    :fin
  end
end
dlreg = Ractor::Port.new
deadletter = Ractor.new(dlreg, done) do |regp, dport|
  inbox = Ractor::Port.new
  regp.send(inbox)
  bad = []
  loop do
    m = inbox.receive
    break if m == :stop
    GC.start if bad.size == 1
    bad << m
  end
  dport.send([:dl, bad.sort])
  :fin
end
wports = Array.new(2)
2.times do
  wid, p = reg.receive
  wports[wid] = p
end
dlport = dlreg.receive
router = Ractor.new(wports, dlport) do |wp, dl|
  table = { a: 0, b: 1 }
  loop do
    m = Ractor.receive
    if m == :stop
      wp.each { |p| p.send(:stop) }
      dl.send(:stop)
      break
    end
    key, payload = m
    idx = table[key]
    idx ? wp[idx].send(payload) : dl.send("#{key}/#{payload}")
  end
  :fin
end
stream = [[:a, 1], [:zz, 2], [:b, 3], [:qq, 4], [:a, 5], [:zz, 6]]
stream.each { |m| router.send(m) }
router.send(:stop)
results = {}
3.times do
  k, v = done.receive
  results[k] = v
end
raise unless results[0] == 2 && results[1] == 1
raise "dl #{results[:dl]}" unless results[:dl] == ["qq/4", "zz/2", "zz/6"]
GC.stress = false
raise unless router.value == :fin
raise unless deadletter.value == :fin
workers.each { |r| raise unless r.value == :fin }
puts "OK e52_route_deadletter"
