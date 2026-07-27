# content-based routing: router dispatches [key, val] to one of 3 workers by key symbol
# axes: router ractor with port table, :stop cascade teardown, per-worker sums
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

KEYS = %i[alpha beta gamma].freeze
reg = Ractor::Port.new
done = Ractor::Port.new
workers = 3.times.map do |w|
  Ractor.new(w, reg, done) do |wid, regp, dport|
    inbox = Ractor::Port.new
    regp.send([wid, inbox])
    sum = 0
    cnt = 0
    loop do
      m = inbox.receive
      break if m == :stop
      sum += m
      cnt += 1
    end
    dport.send([wid, cnt, sum])
    :fin
  end
end
wports = Array.new(3)
3.times do
  wid, p = reg.receive
  wports[wid] = p
end
router = Ractor.new(wports) do |wp|
  table = { alpha: 0, beta: 1, gamma: 2 }
  loop do
    m = Ractor.receive
    if m == :stop
      wp.each { |p| p.send(:stop) }
      break
    end
    key, val = m
    wp[table.fetch(key)].send(val)
  end
  :fin
end
jobs = 18.times.map { |k| [KEYS[k % 3], k * k] }
jobs.each { |j| router.send(j) }
router.send(:stop)
exp = Array.new(3) { |w| jobs.select { |k, _| k == KEYS[w] }.map { |_, v| v } }
3.times do
  wid, cnt, sum = done.receive
  raise "worker #{wid}" unless cnt == exp[wid].size && sum == exp[wid].sum
end
GC.stress = false
raise unless router.value == :fin
workers.each { |r| raise unless r.value == :fin }
puts "OK e45_route_bykey"
