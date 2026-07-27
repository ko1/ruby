# gen4 worker-pool: same shape as wp_basic_copy but every worker runs GC.start
# every 40 jobs and main runs GC.start between dispatch batches.
# axes: transfer=copy, GC=periodic GC.start (workers + main), exceptions=none, payload=mixed
N_WORKERS = 4
N_JOBS = 320

results = Ractor::Port.new
workers = N_WORKERS.times.map do |wid|
  Ractor.new(results, wid) do |res, id|
    done = 0
    while (job = Ractor.receive) != :stop
      tmp = job[:body].map { |x| x.to_s * 2 }
      done += 1
      GC.start if done % 40 == 0
      res << [job[:seq], tmp.sum(&:size)]
    end
    done
  end
end

expected = 0
N_JOBS.times do |i|
  body = [i, "s#{i}", [i, i], { k: i }]
  expected += body.sum { |x| x.to_s.size * 2 }
  workers[i % N_WORKERS] << { seq: i, body: body }
  GC.start if i % 80 == 79
end
workers.each { |w| w << :stop }

got = 0
N_JOBS.times { got += results.receive[1] }
GC.start
raise "FAIL done" unless workers.sum(&:value) == N_JOBS
raise "FAIL #{got} != #{expected}" unless got == expected
puts "OK wp_gc_periodic"
