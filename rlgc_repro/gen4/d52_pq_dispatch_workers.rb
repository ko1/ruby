# Priority dispatch: main preloads PQ service, then dispatcher pops in priority
# order and alternates 2 workers; workers report to main port; global completion
# order preserved via per-task seq check. Axes: copy, stress in workers.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
results = Ractor::Port.new
done = Ractor::Port.new
workers = 2.times.map do |wi|
  Ractor.new(wi, results, done, STRESS) do |wi, results, done, stress|
    GC.stress = true if stress
    loop do
      msg = Ractor.receive
      break if msg == :stop
      rank, tid = msg
      results << [rank, tid, wi]
    end
    GC.stress = false
    done << :done
    :w
  end
end
rng = Random.new(52)
tasks = Array.new(60) { |i| [rng.rand(100), i] }
pq = Ractor.new(workers, tasks, done) do |workers, tasks, done|
  q = tasks.sort # by [prio, id]
  rank = 0
  q.each do |_, tid|
    workers[rank % 2].send([rank, tid])
    rank += 1
  end
  workers.each { _1.send(:stop) }
  done << :done
  rank
end
order = tasks.sort.map { |_, tid| tid }
seen = {}
by_worker = Hash.new(0)
60.times do
  rank, tid, wi = results.receive
  raise "rank/tid" unless order[rank] == tid
  raise "worker parity" unless rank % 2 == wi
  raise "dup" if seen[tid]
  seen[tid] = true
  by_worker[wi] += 1
end
3.times { done.receive }
raise unless pq.value == 60
raise "balance" unless by_worker == { 0 => 30, 1 => 30 }
workers.each { raise unless _1.value == :w }
puts "OK d52_pq_dispatch_workers"
