# gen4 supervisor: job buffers are MOVED into workers; a poison buffer kills
# the worker after the move (buffer dies with the worker's heap). Supervisor
# respawns and re-sends a fresh copy of the lost job.
# axes: transfer=move, GC=GC.start after respawn, exceptions=worker death + respawn
N_JOBS = 40

results = Ractor::Port.new

mk_worker = lambda do
  Ractor.new(results) do |res|
    Thread.current.report_on_exception = false
    n = 0
    while (buf = Ractor.receive) != :stop
      raise "poison" if buf.start_with?("POISON")
      buf << "|done"
      n += 1
      res.send(buf, move: true)
    end
    n
  end
end

mk_payload = lambda do |i, poisoned|
  (poisoned ? "POISON-#{i}:" : "job-#{i}:") + ("d" * (10 + i % 30))
end

poison_ids = (0...N_JOBS).select { |i| i % 9 == 4 }

worker = mk_worker.call
sent_ok = 0
deaths = 0
i = 0
pending_retry = []
while i < N_JOBS || !pending_retry.empty?
  if !pending_retry.empty?
    id = pending_retry.shift
    worker.send(mk_payload.call(id, false), move: true) # retried clean
    sent_ok += 1
  else
    id = i
    i += 1
    poisoned = poison_ids.include?(id)
    worker.send(mk_payload.call(id, poisoned), move: true)
    if poisoned
      begin
        worker.join
      rescue Ractor::RemoteError => e
        raise "wrong" unless e.cause.message == "poison"
      end
      deaths += 1
      GC.start
      worker = mk_worker.call
      pending_retry << id
    else
      sent_ok += 1
    end
  end
end
worker << :stop

got = 0
sent_ok.times do
  s = results.receive
  raise "FAIL marker" unless s.end_with?("|done")
  got += 1
end
raise "FAIL final worker" unless worker.value >= 1
raise "FAIL deaths #{deaths}" unless deaths == poison_ids.size
raise "FAIL got #{got}" unless got == N_JOBS
puts "OK sv_move_poison"
