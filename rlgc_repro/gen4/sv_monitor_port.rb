# gen4 supervisor: detection via Ractor#monitor with ONE monitor port per
# worker; Ractor.select over the ports identifies which worker terminated
# (:exited/:aborted). Aborted workers are respawned; results via #value.
# axes: transfer=copy, GC=none, exceptions=worker death + respawn, payload=small
N_TASKS = 16

mk_worker = lambda do |task_id, attempt|
  r = Ractor.new(task_id, attempt) do |tid, att|
    Thread.current.report_on_exception = false
    raise "flaky #{tid}" if att == 0 && tid % 3 == 0
    (0..tid).sum * (att + 1)
  end
  mon = Ractor::Port.new
  r.monitor(mon)
  [r, mon]
end

live = {}  # mon_port => [ractor, task_id, attempt]
N_TASKS.times do |t|
  r, mon = mk_worker.call(t, 0)
  live[mon] = [r, t, 0]
end

done = {}
aborts = 0
until live.empty?
  port, event = Ractor.select(*live.keys)
  r, tid, att = live.delete(port)
  case event
  when :exited
    raise "dup #{tid}" if done.key?(tid)
    done[tid] = r.value
  when :aborted
    begin
      r.value
      raise "aborted but value ok?"
    rescue Ractor::RemoteError => e
      raise "unexpected: #{e.cause}" unless e.cause.message == "flaky #{tid}"
    end
    aborts += 1
    nr, nmon = mk_worker.call(tid, att + 1)
    live[nmon] = [nr, tid, att + 1]
  else
    raise "unknown event #{event.inspect}"
  end
end

expected_aborts = (0...N_TASKS).count { |t| t % 3 == 0 }
raise "FAIL aborts #{aborts}" unless aborts == expected_aborts
N_TASKS.times do |t|
  exp = (0..t).sum * (t % 3 == 0 ? 2 : 1)
  raise "FAIL task #{t}: #{done[t]} != #{exp}" unless done[t] == exp
end
puts "OK sv_monitor_port"
