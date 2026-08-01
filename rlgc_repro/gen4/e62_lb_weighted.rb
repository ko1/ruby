# weighted balancing: deterministic schedule [0,0,0,1,1,2] repeated; weights 3:2:1
# axes: static weighted rotation, per-worker exact counts and sums
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

SCHED = [0, 0, 0, 1, 1, 2].freeze
J = 18
reg = Ractor::Port.new
done = Ractor::Port.new
workers = 3.times.map do |w|
  quota = J / SCHED.size * SCHED.count(w)
  Ractor.new(w, reg, done, quota) do |wid, regp, dport, q|
    inbox = Ractor::Port.new
    regp.send([wid, inbox])
    sum = 0
    q.times { sum += inbox.receive }
    dport.send([wid, q, sum])
    :fin
  end
end
wports = Array.new(3)
3.times do
  wid, p = reg.receive
  wports[wid] = p
end
dispatcher = Ractor.new(wports, SCHED, J) do |wp, sched, jobs|
  jobs.times { |k| wp[sched[k % sched.size]].send(k + 1) }
  :disp_fin
end
exp_sum = Array.new(3, 0)
J.times { |k| exp_sum[SCHED[k % SCHED.size]] += k + 1 }
3.times do
  wid, cnt, sum = done.receive
  raise "worker #{wid} cnt" unless cnt == J / SCHED.size * SCHED.count(wid)
  raise "worker #{wid} sum" unless sum == exp_sum[wid]
end
GC.stress = false
raise unless dispatcher.value == :disp_fin
workers.each { |r| raise unless r.value == :fin }
puts "OK e62_lb_weighted"
