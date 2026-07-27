# gen4 worker-pool: small pool where ONE worker runs with GC.stress=true for
# the whole run. Kept deliberately tiny so the stressed ractor stays fast.
# axes: transfer=copy, GC=GC.stress in 1 ractor, exceptions=none, payload=small strings
N_WORKERS = 3
N_JOBS = 90

results = Ractor::Port.new
workers = N_WORKERS.times.map do |wid|
  Ractor.new(results, wid) do |res, id|
    GC.stress = true if id == 0
    done = 0
    while (job = Ractor.receive) != :stop
      s = job[:body].chars.sort.join
      done += 1
      res << [job[:seq], s.size]
    end
    GC.stress = false if id == 0
    done
  end
end

expected = 0
N_JOBS.times do |i|
  body = "abc#{i}" * (1 + i % 3)
  expected += body.size
  workers[i % N_WORKERS] << { seq: i, body: body }
end
workers.each { |w| w << :stop }

got = 0
N_JOBS.times { got += results.receive[1] }
raise "FAIL done" unless workers.sum(&:value) == N_JOBS
raise "FAIL #{got} != #{expected}" unless got == expected
puts "OK wp_stress_small"
