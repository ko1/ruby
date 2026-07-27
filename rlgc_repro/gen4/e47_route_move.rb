# routing with move end-to-end: main moves job to router, router re-moves to worker by key
# axes: double move through an intermediary, worker mutates and reports length sums
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

reg = Ractor::Port.new
done = Ractor::Port.new
workers = 2.times.map do |w|
  Ractor.new(w, reg, done) do |wid, regp, dport|
    inbox = Ractor::Port.new
    regp.send([wid, inbox])
    total = 0
    loop do
      m = inbox.receive
      break if m == :stop
      m[:body] << "!"
      total += m[:body].length
    end
    dport.send([wid, total])
    :fin
  end
end
wports = Array.new(2)
2.times do
  wid, p = reg.receive
  wports[wid] = p
end
router = Ractor.new(wports) do |wp|
  loop do
    m = Ractor.receive
    if m == :stop
      wp.each { |p| p.send(:stop) }
      break
    end
    wp[m[:key]].send(m, move: true)
  end
  :fin
end
lens = [[], []]
10.times do |k|
  key = k % 2
  body = "b" * (k + 1)
  lens[key] << body.length + 1
  router.send({ key: key, body: body }, move: true)
end
router.send(:stop)
2.times do
  wid, total = done.receive
  raise "worker #{wid}" unless total == lens[wid].sum
end
GC.stress = false
raise unless router.value == :fin
workers.each { |r| raise unless r.value == :fin }
puts "OK e47_route_move"
