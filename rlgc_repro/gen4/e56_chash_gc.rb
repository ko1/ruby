# consistent-hash routing with GC.compact at router midway and GC.start at workers per 4 msgs
# axes: compaction at hot router while hashed stream in flight
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

def dh4(s)
  s.bytes.inject(5) { |a, b| (a * 139 + b) % 211 }
end

NODES = 4
J = 16
reg = Ractor::Port.new
done = Ractor::Port.new
workers = NODES.times.map do |w|
  Ractor.new(w, reg, done) do |wid, regp, dport|
    inbox = Ractor::Port.new
    regp.send([wid, inbox])
    cnt = 0
    sum = 0
    loop do
      m = inbox.receive
      break if m == :stop
      cnt += 1
      sum += dh4(m)
      GC.start if cnt % 4 == 0
    end
    dport.send([wid, cnt, sum])
    :fin
  end
end
wports = Array.new(NODES)
NODES.times do
  wid, p = reg.receive
  wports[wid] = p
end
router = Ractor.new(wports, NODES, J) do |wp, n, jobs|
  routed = 0
  loop do
    m = Ractor.receive
    if m == :stop
      wp.each { |p| p.send(:stop) }
      break
    end
    wp[dh4(m) % n].send(m)
    routed += 1
    GC.compact if routed == jobs / 2
  end
  :fin
end
keys = J.times.map { |k| "gc-key-#{k}" }
keys.each { |k| router.send(k) }
router.send(:stop)
exp_cnt = Array.new(NODES, 0)
exp_sum = Array.new(NODES, 0)
keys.each do |k|
  w = dh4(k) % NODES
  exp_cnt[w] += 1
  exp_sum[w] += dh4(k)
end
NODES.times do
  wid, cnt, sum = done.receive
  raise "worker #{wid}" unless cnt == exp_cnt[wid] && sum == exp_sum[wid]
end
GC.stress = false
raise unless router.value == :fin
workers.each { |r| raise unless r.value == :fin }
puts "OK e56_chash_gc"
