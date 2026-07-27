# Round-robin scheduler: dispatcher assigns 60 tasks to 3 workers in rotation;
# workers post results to main's port; per-worker counts exact.
# Axes: dispatcher+3 workers, copy, stress in workers.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
results = Ractor::Port.new
done = Ractor::Port.new
workers = 3.times.map do |wi|
  Ractor.new(wi, results, done, STRESS) do |wi, results, done, stress|
    GC.stress = true if stress
    loop do
      msg = Ractor.receive
      break if msg == :stop
      tid, x = msg
      results << [wi, tid, x * x + wi]
    end
    GC.stress = false
    done << :done
    :w
  end
end
sched = Ractor.new(workers, done) do |workers, done|
  i = 0
  loop do
    msg = Ractor.receive
    break if msg == :stop
    workers[i % 3].send(msg)
    i += 1
  end
  workers.each { |w| w.send(:stop) }
  done << :done
  i
end
60.times { |t| sched.send([t, t + 1]) }
sched.send(:stop)
got = {}
per_worker = Hash.new(0)
60.times do
  wi, tid, r = results.receive
  raise "dup #{tid}" if got.key?(tid)
  got[tid] = true
  per_worker[wi] += 1
  raise "result" unless r == (tid + 1) * (tid + 1) + wi
  raise "rr" unless tid % 3 == wi
end
4.times { done.receive }
raise unless sched.value == 60
raise "balance" unless per_worker == { 0 => 20, 1 => 20, 2 => 20 }
workers.each { raise unless _1.value == :w }
puts "OK d36_sched_round_robin"
