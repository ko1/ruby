# load balancing round-robin: dispatcher deals 20 jobs over 4 workers in strict rotation
# axes: fully deterministic per-worker job lists, results returned via worker->main port
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

W = 4
J = 20
reg = Ractor::Port.new
done = Ractor::Port.new
workers = W.times.map do |w|
  Ractor.new(w, reg, done, J / W) do |wid, regp, dport, quota|
    inbox = Ractor::Port.new
    regp.send([wid, inbox])
    got = []
    quota.times { got << inbox.receive }
    dport.send([wid, got])
    :fin
  end
end
wports = Array.new(W)
W.times do
  wid, p = reg.receive
  wports[wid] = p
end
dispatcher = Ractor.new(wports, J) do |wp, jobs|
  jobs.times { |k| wp[k % wp.size].send(k * 3) }
  :disp_fin
end
W.times do
  wid, got = done.receive
  exp = (0...J).select { |k| k % W == wid }.map { |k| k * 3 }
  raise "worker #{wid}: #{got}" unless got == exp
end
GC.stress = false
raise unless dispatcher.value == :disp_fin
workers.each { |r| raise unless r.value == :fin }
puts "OK e59_lb_rr"
