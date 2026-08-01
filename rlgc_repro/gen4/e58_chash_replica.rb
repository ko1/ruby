# consistent-hash with replication: each key stored on primary (h%N) and replica ((h+1)%N)
# axes: dual routing per key, replica sets verified disjointly, GC.compact at one worker
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

def dh5(s)
  s.bytes.inject(13) { |a, b| (a * 149 + b) % 1009 }
end

NODES = 5
reg = Ractor::Port.new
done = Ractor::Port.new
workers = NODES.times.map do |w|
  Ractor.new(w, reg, done) do |wid, regp, dport|
    inbox = Ractor::Port.new
    regp.send([wid, inbox])
    prim = []
    repl = []
    loop do
      m = inbox.receive
      break if m == :stop
      role, key = m
      (role == :p ? prim : repl) << key
    end
    GC.compact if wid == 0
    dport.send([wid, prim.sort, repl.sort])
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
    h = dh5(m)
    wp[h % n].send([:p, m])
    wp[(h + 1) % n].send([:r, m])
  end
  :fin
end
keys = 15.times.map { |k| "rep-#{k}" }
keys.each { |k| router.send(k) }
router.send(:stop)
exp_p = Array.new(NODES) { [] }
exp_r = Array.new(NODES) { [] }
keys.each do |k|
  h = dh5(k)
  exp_p[h % NODES] << k
  exp_r[(h + 1) % NODES] << k
end
NODES.times do
  wid, prim, repl = done.receive
  raise "worker #{wid} prim" unless prim == exp_p[wid].sort
  raise "worker #{wid} repl" unless repl == exp_r[wid].sort
end
GC.stress = false
raise unless router.value == :fin
workers.each { |r| raise unless r.value == :fin }
puts "OK e58_chash_replica"
