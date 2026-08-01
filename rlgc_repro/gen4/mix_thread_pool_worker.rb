# gen4 mixed-runtime: each ractor worker runs an INTERNAL pool of 4 threads
# fed by a Thread::Queue; jobs arrive from main via the ractor's default port
# and results leave via a shared result port.
# axes: transfer=copy, GC=none, exceptions=none, runtime=threads inside ractors
N_RACTORS = 3
N_THREADS = 4
JOBS_PER_RACTOR = 120

results = Ractor::Port.new
workers = N_RACTORS.times.map do |wid|
  Ractor.new(results, wid, N_THREADS, JOBS_PER_RACTOR) do |res, id, nt, njobs|
    q = Thread::Queue.new
    outq = Thread::Queue.new
    threads = nt.times.map do
      Thread.new do
        while (job = q.pop) != :stop
          outq << job[:vals].sum * job[:mult]
        end
      end
    end
    collector = Thread.new do
      acc = 0
      njobs.times { acc += outq.pop }
      acc
    end
    njobs.times { q << Ractor.receive }
    nt.times { q << :stop }
    threads.each(&:join)
    total = collector.value
    res << [id, total]
    total
  end
end

exp = Array.new(N_RACTORS, 0)
JOBS_PER_RACTOR.times do |i|
  N_RACTORS.times do |w|
    vals = [i, w + 1, i % 5]
    exp[w] += vals.sum * 2
    workers[w] << { vals: vals, mult: 2 }
  end
end

got = Array.new(N_RACTORS, 0)
N_RACTORS.times do
  id, total = results.receive
  got[id] = total
end
raise "FAIL #{got} != #{exp}" unless got == exp
raise "FAIL values" unless workers.map(&:value) == got.each_with_index.map { |v, _| v }
puts "OK mix_thread_pool_worker"
