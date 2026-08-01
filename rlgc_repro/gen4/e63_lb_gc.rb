# round-robin balancing with GC.start at dispatcher every 4 jobs, GC.compact at workers at end
# axes: GC alternating between dispatcher and workers, array payloads
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

W = 4
J = 16
reg = Ractor::Port.new
done = Ractor::Port.new
workers = W.times.map do |w|
  Ractor.new(w, reg, done, J / W) do |wid, regp, dport, quota|
    inbox = Ractor::Port.new
    regp.send([wid, inbox])
    acc = 0
    quota.times do
      a = inbox.receive
      acc += a.sum
    end
    GC.compact if wid.odd?
    dport.send([wid, acc])
    :fin
  end
end
wports = Array.new(W)
W.times do
  wid, p = reg.receive
  wports[wid] = p
end
dispatcher = Ractor.new(wports, J) do |wp, jobs|
  jobs.times do |k|
    wp[k % wp.size].send([k, k * 2, k * 3])
    GC.start if k % 4 == 3
  end
  :disp_fin
end
W.times do
  wid, acc = done.receive
  exp = (0...J).select { |k| k % W == wid }.sum { |k| k * 6 }
  raise "worker #{wid}" unless acc == exp
end
GC.stress = false
raise unless dispatcher.value == :disp_fin
workers.each { |r| raise unless r.value == :fin }
puts "OK e63_lb_gc"
