# consistent-hash-style routing: nodes at positions (i*97)%251 on a ring, keys go clockwise
# axes: deterministic byte-fold hash (no String#hash), router + 5 bucket workers
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

NODES = 5
def dhash(s)
  s.bytes.inject(7) { |a, b| (a * 131 + b) % 251 }
end

def owner(key, positions)
  h = dhash(key)
  cand = positions.select { |pos, _i| pos >= h }
  (cand.min_by(&:first) || positions.min_by(&:first))[1]
end

positions = NODES.times.map { |i| [(i * 97) % 251, i] }
reg = Ractor::Port.new
done = Ractor::Port.new
workers = NODES.times.map do |w|
  Ractor.new(w, reg, done) do |wid, regp, dport|
    inbox = Ractor::Port.new
    regp.send([wid, inbox])
    keys = []
    loop do
      m = inbox.receive
      break if m == :stop
      keys << m
    end
    dport.send([wid, keys.sort])
    :fin
  end
end
wports = Array.new(NODES)
NODES.times do
  wid, p = reg.receive
  wports[wid] = p
end
router = Ractor.new(wports, positions) do |wp, pos|
  loop do
    m = Ractor.receive
    if m == :stop
      wp.each { |p| p.send(:stop) }
      break
    end
    wp[owner(m, pos)].send(m)
  end
  :fin
end
keys = 25.times.map { |k| "key-#{k}" }
keys.each { |k| router.send(k) }
router.send(:stop)
exp = Array.new(NODES) { [] }
keys.each { |k| exp[owner(k, positions)] << k }
NODES.times do
  wid, got = done.receive
  raise "worker #{wid}: #{got}" unless got == exp[wid].sort
end
GC.stress = false
raise unless router.value == :fin
workers.each { |r| raise unless r.value == :fin }
puts "OK e53_chash_ring"
