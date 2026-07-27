# topic routing: prefix match (start_with?) dispatches topic strings to subscriber workers
# axes: string prefix dispatch, one topic can match several subscribers (fanout)
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

PREFIXES = ["sys.", "sys.disk", "app."].freeze
reg = Ractor::Port.new
done = Ractor::Port.new
workers = 3.times.map do |w|
  Ractor.new(w, reg, done) do |wid, regp, dport|
    inbox = Ractor::Port.new
    regp.send([wid, inbox])
    got = []
    loop do
      m = inbox.receive
      break if m == :stop
      got << m
    end
    dport.send([wid, got.sort])
    :fin
  end
end
wports = Array.new(3)
3.times do
  wid, p = reg.receive
  wports[wid] = p
end
router = Ractor.new(wports, PREFIXES) do |wp, prefixes|
  loop do
    m = Ractor.receive
    if m == :stop
      wp.each { |p| p.send(:stop) }
      break
    end
    prefixes.each_with_index do |pre, i|
      wp[i].send(m) if m.start_with?(pre)
    end
  end
  :fin
end
topics = %w[sys.cpu sys.disk.io app.web sys.disk app.db other.x]
topics.each { |t| router.send(t) }
router.send(:stop)
3.times do
  wid, got = done.receive
  exp = topics.select { |t| t.start_with?(PREFIXES[wid]) }.sort
  raise "worker #{wid}: #{got} != #{exp}" unless got == exp
end
GC.stress = false
raise unless router.value == :fin
workers.each { |r| raise unless r.value == :fin }
puts "OK e51_route_topics"
