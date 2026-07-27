# consistent-hash rebalance: phase 1 with 4 nodes, node 2 removed, phase 2 keys reroute
# axes: topology change mid-stream via router control message, per-phase expected sets
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

def dhash2(s)
  s.bytes.inject(3) { |a, b| (a * 137 + b) % 199 }
end

def owner2(key, positions)
  h = dhash2(key)
  cand = positions.select { |pos, _i| pos >= h }
  (cand.min_by(&:first) || positions.min_by(&:first))[1]
end

NODES = 4
pos_full = NODES.times.map { |i| [(i * 53) % 199, i] }
pos_less = pos_full.reject { |_p, i| i == 2 }
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
router = Ractor.new(wports, pos_full, pos_less) do |wp, full, less|
  pos = full
  loop do
    m = Ractor.receive
    case m
    when :stop
      wp.each { |p| p.send(:stop) }
      break
    when :drop2
      pos = less
    else
      wp[owner2(m, pos)].send(m)
    end
  end
  :fin
end
phase1 = 10.times.map { |k| "p1-#{k}" }
phase2 = 10.times.map { |k| "p2-#{k}" }
phase1.each { |k| router.send(k) }
router.send(:drop2)
phase2.each { |k| router.send(k) }
router.send(:stop)
exp = Array.new(NODES) { [] }
phase1.each { |k| exp[owner2(k, pos_full)] << k }
phase2.each { |k| exp[owner2(k, pos_less)] << k }
NODES.times do
  wid, got = done.receive
  raise "worker #{wid}: #{got} != #{exp[wid].sort}" unless got == exp[wid].sort
end
GC.stress = false
raise unless router.value == :fin
workers.each { |r| raise unless r.value == :fin }
puts "OK e54_chash_remove"
