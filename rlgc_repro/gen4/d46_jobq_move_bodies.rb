# Job queue where job bodies move main->queue->worker and results move back.
# Axes: 1 queue + 1 worker, 50 jobs, move both hops, stress in queue and worker.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
results = Ractor::Port.new
worker = Ractor.new(results, done, STRESS) do |results, done, stress|
  GC.stress = true if stress
  loop do
    msg = Ractor.receive
    break if msg == :stop
    id, body = msg
    out = +"done:#{id}:#{body.sum}"
    results.send([id, out], move: true)
  end
  GC.stress = false
  done << :done
  :w
end
queue = Ractor.new(worker, done, STRESS) do |worker, done, stress|
  GC.stress = true if stress
  n = 0
  loop do
    msg = Ractor.receive
    break if msg == :stop
    n += 1
    worker.send(msg, move: true)
  end
  worker.send(:stop)
  GC.stress = false
  done << :done
  n
end
50.times do |i|
  body = Array.new(10) { |j| i * 10 + j }
  queue.send([i, body], move: true)
end
queue.send(:stop)
seen = {}
50.times do
  id, out = results.receive
  raise "dup" if seen[id]
  seen[id] = true
  want = "done:#{id}:#{(0...10).sum { id * 10 + _1 }}"
  raise "res#{id}" unless out == want
end
2.times { done.receive }
raise unless queue.value == 50 && worker.value == :w
puts "OK d46_jobq_move_bodies"
