# consistent-hash routing with moved record payloads; workers mutate and total body sizes
# axes: move through router by hashed key, records carry mutable string bodies
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

def dh3(s)
  s.bytes.inject(11) { |a, b| (a * 127 + b) % 173 }
end

NODES = 3
reg = Ractor::Port.new
done = Ractor::Port.new
workers = NODES.times.map do |w|
  Ractor.new(w, reg, done) do |wid, regp, dport|
    inbox = Ractor::Port.new
    regp.send([wid, inbox])
    bytes = 0
    loop do
      m = inbox.receive
      break if m == :stop
      m[:body] << "+w#{wid}"
      bytes += m[:body].length
    end
    dport.send([wid, bytes])
    :fin
  end
end
wports = Array.new(NODES)
NODES.times do
  wid, p = reg.receive
  wports[wid] = p
end
router = Ractor.new(wports, NODES) do |wp, n|
  loop do
    m = Ractor.receive
    if m == :stop
      wp.each { |p| p.send(:stop) }
      break
    end
    wp[dh3(m[:key]) % n].send(m, move: true)
  end
  :fin
end
exp = Array.new(NODES, 0)
12.times do |k|
  key = "item#{k}"
  body = "d" * (k + 3)
  w = dh3(key) % NODES
  exp[w] += body.length + "+w#{w}".length
  router.send({ key: key, body: body }, move: true)
end
router.send(:stop)
NODES.times do
  wid, bytes = done.receive
  raise "worker #{wid}: #{bytes} != #{exp[wid]}" unless bytes == exp[wid]
end
GC.stress = false
raise unless router.value == :fin
workers.each { |r| raise unless r.value == :fin }
puts "OK e55_chash_move"
