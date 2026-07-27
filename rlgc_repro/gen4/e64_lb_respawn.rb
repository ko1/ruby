# balancing with worker lifecycle: gen1 workers serve 3 jobs and die; gen2 arrives tagged
# axes: deterministic worker death, dispatcher switches generations via mailbox messages
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

W = 3
CAP = 3
done = Ractor::Port.new

def spawn_worker(gen, idx, dport)
  Ractor.new(gen, idx, dport, CAP) do |g, i, dp, cap|
    sum = 0
    cap.times { sum += Ractor.receive }
    dp.send([g, i, sum])
    :fin
  end
end

gen1 = W.times.map { |i| spawn_worker(1, i, done) }
dispatcher = Ractor.new(gen1, W, CAP) do |ws, w, cap|
  ws.each_with_index do |r, i|
    cap.times { |k| r.send(100 + i * 10 + k) }
  end
  gen2 = []
  w.times do
    tag, r = Ractor.receive
    raise unless tag == :new
    gen2 << r
  end
  gen2.each_with_index do |r, i|
    cap.times { |k| r.send(200 + i * 10 + k) }
  end
  :disp_fin
end
got1 = {}
W.times do
  g, i, sum = done.receive
  raise unless g == 1
  got1[i] = sum
end
W.times { |i| raise "gen1 #{i}" unless got1[i] == (0...CAP).sum { |k| 100 + i * 10 + k } }
gen2 = W.times.map { |i| spawn_worker(2, i, done) }
gen2.each { |r| dispatcher.send([:new, r]) }
got2 = {}
W.times do
  g, i, sum = done.receive
  raise unless g == 2
  got2[i] = sum
end
W.times { |i| raise "gen2 #{i}" unless got2[i] == (0...CAP).sum { |k| 200 + i * 10 + k } }
GC.stress = false
raise unless dispatcher.value == :disp_fin
(gen1 + gen2).each { |r| raise unless r.value == :fin }
puts "OK e64_lb_respawn"
