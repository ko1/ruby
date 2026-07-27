# gen4 supervisor: job-level retry queue. Pool workers pull jobs; a worker
# DIES when it draws a cursed job on its first sighting. The supervisor
# requeues the failed job (marked retry) and respawns the worker slot.
# axes: transfer=copy, GC=occasional GC.start, exceptions=worker death + job retry
N_JOBS = 60

jobs = (0...N_JOBS).map { |i| { id: i, retry: false } }
cursed = (0...N_JOBS).select { |i| i % 8 == 5 }

request = Ractor::Port.new

mk_worker = lambda do |wid|
  Ractor.new(request, wid) do |req, id|
    Thread.current.report_on_exception = false
    inbox = Ractor::Port.new
    sum = 0
    loop do
      req << [:need, id, inbox]
      job = inbox.receive
      break if job == :stop
      raise "cursed #{job[:id]}" if job[:cursed] && !job[:retry]
      sum += job[:id]
    end
    sum
  end
end

pool = {}  # wid => [ractor, current_job or nil / :stopping]
4.times { |w| pool[w] = [mk_worker.call(w), nil] }

queue = jobs.map { |j| j.merge(cursed: cursed.include?(j[:id])) }
done_sum = 0
deaths = 0
completed = 0
stopped = 0

while stopped < pool.size
  # watch request port AND non-stopping workers (a dead worker sends no request)
  watch = pool.values.reject { |_, st| st == :stopping }.map(&:first)
  begin
    which, msg = Ractor.select(request, *watch)
    raise "unexpected ractor result" unless which == request
    _tag, wid, inbox = msg
    # previous job of this worker finished successfully
    if (prev = pool[wid][1])
      done_sum += prev[:id]
      completed += 1
      pool[wid][1] = nil
    end
    if queue.empty?
      inbox << :stop
      stopped += 1
      pool[wid][1] = :stopping
    else
      job = queue.shift
      pool[wid][1] = job
      inbox << job
    end
  rescue Ractor::RemoteError => e
    wid = pool.find { |_, (r, _)| r == e.ractor }.first
    failed_job = pool[wid][1]
    raise "death without job" unless failed_job
    deaths += 1
    queue << failed_job.merge(retry: true)
    GC.start if deaths % 3 == 0
    pool[wid] = [mk_worker.call(wid), nil]
  end
end
pool.each_value { |r, _| r.join }

raise "FAIL deaths #{deaths}" unless deaths == cursed.size
raise "FAIL completed #{completed}" unless completed == N_JOBS
raise "FAIL sum" unless done_sum == (0...N_JOBS).sum
puts "OK sv_flaky_jobs"
