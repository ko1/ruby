# gen4 worker-pool: large string buffers MOVED to workers; workers mutate in
# place and MOVE the result back through the shared result port.
# axes: transfer=move (both directions), GC=none, exceptions=none, payload=large strings
N_WORKERS = 4
N_JOBS = 200

results = Ractor::Port.new
workers = N_WORKERS.times.map do |wid|
  Ractor.new(results, wid) do |res, id|
    n = 0
    while (job = Ractor.receive) != :stop
      job << "|worker#{id}"
      job.upcase!
      n += 1
      res.send(job, move: true)
    end
    n
  end
end

expected_len = 0
N_JOBS.times do |i|
  payload = "payload-#{i}:" + ("abc" * (50 + i % 40))
  expected_len += payload.length # suffix length added separately below
  workers[i % N_WORKERS].send(payload, move: true)
end
workers.each { |w| w << :stop }

got_len = 0
N_JOBS.times do
  s = results.receive
  raise "FAIL not upcased: #{s[0, 20]}" unless s == s.upcase
  raise "FAIL missing marker" unless s.include?("|WORKER")
  got_len += s.length - "|workerN".length
end
raise "FAIL jobs" unless workers.sum(&:value) == N_JOBS
raise "FAIL len #{got_len} != #{expected_len}" unless got_len == expected_len
puts "OK wp_move_strings"
