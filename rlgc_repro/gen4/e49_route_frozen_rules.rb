# router using a frozen shareable rules table (make_shareable) mapping key strings to workers
# axes: shareable routing rules as constructor arg, string keys, counted per-worker
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

RULES = Ractor.make_shareable({
  "orders" => 0, "billing" => 1, "audit" => 2, "ship" => 0, "refund" => 1,
}.freeze)

reg = Ractor::Port.new
done = Ractor::Port.new
workers = 3.times.map do |w|
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
wports = Array.new(3)
3.times do
  wid, p = reg.receive
  wports[wid] = p
end
router = Ractor.new(wports, RULES) do |wp, rules|
  loop do
    m = Ractor.receive
    if m == :stop
      wp.each { |p| p.send(:stop) }
      break
    end
    wp[rules.fetch(m)].send(m)
  end
  :fin
end
stream = %w[orders billing audit ship refund orders audit]
stream.each { |k| router.send(k) }
router.send(:stop)
exp = Array.new(3) { |w| stream.select { |k| RULES[k] == w }.sort }
3.times do
  wid, keys = done.receive
  raise "worker #{wid}: #{keys}" unless keys == exp[wid]
end
GC.stress = false
raise unless router.value == :fin
workers.each { |r| raise unless r.value == :fin }
puts "OK e49_route_frozen_rules"
