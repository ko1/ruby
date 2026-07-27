# batch balancing: dispatcher sends a full batch to all workers, waits for every ack, repeats
# axes: batch barrier via counted acks, 4 batches x 3 workers, batch ids verified
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

W = 3
B = 4
reg = Ractor::Port.new
done = Ractor::Port.new
workers = W.times.map do |w|
  Ractor.new(w, reg, done, B) do |wid, regp, dport, batches|
    inbox = Ractor::Port.new
    regp.send([wid, inbox])
    seen = []
    batches.times do
      ackp, bid, v = inbox.receive
      seen << bid
      ackp.send([wid, v + wid])
    end
    dport.send([wid, seen])
    :fin
  end
end
wports = Array.new(W)
W.times do
  wid, p = reg.receive
  wports[wid] = p
end
dispatcher = Ractor.new(wports, B) do |wp, batches|
  ackp = Ractor::Port.new
  results = []
  batches.times do |b|
    wp.each { |p| p.send([ackp, b, b * 10]) }
    acks = []
    wp.size.times { acks << ackp.receive }
    results << acks.map { |wid, v| v }.sum
  end
  results
end
W.times do
  wid, seen = done.receive
  raise "worker #{wid}: #{seen}" unless seen == (0...B).to_a
end
GC.stress = false
res = dispatcher.value
exp = (0...B).map { |b| W * (b * 10) + (0...W).sum }
raise "results #{res}" unless res == exp
workers.each { |r| raise unless r.value == :fin }
puts "OK e65_lb_batch_ack"
