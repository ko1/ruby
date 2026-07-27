# sticky balancing: jobs with client keys always land on key%W worker; sessions accumulate
# axes: key affinity, per-worker session logs verified, frozen client list
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

W = 3
CLIENTS = %w[carol dave erin frank grace heidi].freeze
def ckey(name)
  name.bytes.sum
end

reg = Ractor::Port.new
done = Ractor::Port.new
workers = W.times.map do |w|
  Ractor.new(w, reg, done) do |wid, regp, dport|
    inbox = Ractor::Port.new
    regp.send([wid, inbox])
    sessions = Hash.new(0)
    loop do
      m = inbox.receive
      break if m == :stop
      name, amt = m
      sessions[name] += amt
    end
    dport.send([wid, sessions.keys.sort, sessions.values.sum])
    :fin
  end
end
wports = Array.new(W)
W.times do
  wid, p = reg.receive
  wports[wid] = p
end
lb = Ractor.new(wports) do |wp|
  loop do
    m = Ractor.receive
    if m == :stop
      wp.each { |p| p.send(:stop) }
      break
    end
    wp[ckey(m[0]) % wp.size].send(m)
  end
  :fin
end
jobs = []
3.times { |r| CLIENTS.each { |c| jobs << [c, r + 1] } }
jobs.each { |j| lb.send(j) }
lb.send(:stop)
exp_names = Array.new(W) { |w| CLIENTS.select { |c| ckey(c) % W == w }.sort }
exp_sum = Array.new(W) { |w| exp_names[w].size * 6 }
W.times do
  wid, names, sum = done.receive
  raise "worker #{wid} names" unless names == exp_names[wid]
  raise "worker #{wid} sum" unless sum == exp_sum[wid]
end
GC.stress = false
raise unless lb.value == :fin
workers.each { |r| raise unless r.value == :fin }
puts "OK e66_lb_sticky"
