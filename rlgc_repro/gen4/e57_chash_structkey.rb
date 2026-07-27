# consistent-hash routing keyed by Struct(ns, id): hash folded from both fields
# axes: Struct keys, 4 buckets, worker echoes reconstructed key strings for verification
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

Key = Struct.new(:ns, :id)
def khash(k)
  k.ns.bytes.inject(k.id * 31 + 1) { |a, b| (a * 131 + b) % 1_000_003 }
end

NODES = 4
reg = Ractor::Port.new
done = Ractor::Port.new
workers = NODES.times.map do |w|
  Ractor.new(w, reg, done) do |wid, regp, dport|
    inbox = Ractor::Port.new
    regp.send([wid, inbox])
    sigs = []
    loop do
      m = inbox.receive
      break if m == :stop
      sigs << "#{m.ns}/#{m.id}"
    end
    dport.send([wid, sigs.sort])
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
    wp[khash(m) % n].send(m)
  end
  :fin
end
keys = []
%w[users posts tags].each { |ns| 5.times { |i| keys << Key.new(ns, i) } }
keys.each { |k| router.send(k) }
router.send(:stop)
exp = Array.new(NODES) { [] }
keys.each { |k| exp[khash(k) % NODES] << "#{k.ns}/#{k.id}" }
NODES.times do
  wid, sigs = done.receive
  raise "worker #{wid}: #{sigs}" unless sigs == exp[wid].sort
end
GC.stress = false
raise unless router.value == :fin
workers.each { |r| raise unless r.value == :fin }
puts "OK e57_chash_structkey"
