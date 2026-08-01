# round-robin balancing with moved job objects; workers mutate jobs and move results to main
# axes: move on dispatch and on result return, hash job records
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

W = 3
J = 12
reg = Ractor::Port.new
done = Ractor::Port.new
workers = W.times.map do |w|
  Ractor.new(w, reg, done, J / W) do |wid, regp, dport, quota|
    inbox = Ractor::Port.new
    regp.send([wid, inbox])
    quota.times do
      job = inbox.receive
      job[:result] = job[:input] * 2 + wid
      job[:done_by] = wid
      dport.send(job, move: true)
    end
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
    wp[k % wp.size].send({ id: k, input: k * 5, result: nil, done_by: nil }, move: true)
  end
  :disp_fin
end
J.times do
  job = done.receive
  exp_w = job[:id] % W
  raise "job #{job[:id]}" unless job[:done_by] == exp_w && job[:result] == job[:input] * 2 + exp_w
end
GC.stress = false
raise unless dispatcher.value == :disp_fin
workers.each { |r| raise unless r.value == :fin }
puts "OK e61_lb_rr_move"
