# gen4 worker-pool: pull-based dispatch. Each worker owns a private job port,
# registers it with main via a request port, and pulls one job at a time.
# Per-worker sums returned via #value.
# axes: transfer=copy, GC=none, exceptions=none, payload=small arrays
N_WORKERS = 6
N_JOBS = 360

request = Ractor::Port.new
workers = N_WORKERS.times.map do |wid|
  Ractor.new(request, wid) do |req, id|
    inbox = Ractor::Port.new
    sum = 0
    loop do
      req << [:need, id, inbox]
      job = inbox.receive
      break if job == :stop
      sum += job.each_with_index.sum { |v, k| v * (k + 1) }
    end
    [id, sum]
  end
end

expected = 0
sent = 0
stopped = 0
while stopped < N_WORKERS
  _tag, _wid, inbox = request.receive
  if sent < N_JOBS
    job = [sent, sent + 1, sent % 13]
    expected += job.each_with_index.sum { |v, k| v * (k + 1) }
    inbox << job
    sent += 1
  else
    inbox << :stop
    stopped += 1
  end
end

total = workers.sum { |w| w.value[1] }
raise "FAIL #{total} != #{expected}" unless total == expected
puts "OK wp_pull_dispatch"
