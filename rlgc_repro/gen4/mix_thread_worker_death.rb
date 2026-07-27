# gen4 mixed-runtime + supervisor: workers run internal threads; in one
# attempt a worker's thread raises and the exception propagates out of the
# ractor (via Thread#value re-raise) killing it mid-batch; supervisor
# respawns. Mixes thread teardown with ractor death.
# axes: transfer=copy, GC=GC.start on respawn, exceptions=thread raise -> ractor death, runtime=threads
N_WORKERS = 4
BATCH = 80

mk_worker = lambda do |wid, attempt|
  Ractor.new(wid, attempt, BATCH) do |id, att, batch|
    Thread.current.report_on_exception = false
    halves = 2.times.map do |h|
      Thread.new(h) do |hh|
        Thread.current.report_on_exception = false
        sum = 0
        (batch / 2).times do |i|
          raise "thread blew up in w#{id}" if att == 0 && id == 2 && hh == 1 && i == 10
          sum += id * 10 + hh + i
        end
        sum
      end
    end
    halves.sum(&:value)   # re-raises the thread's exception -> kills ractor
  end
end

live = {}
N_WORKERS.times { |w| live[mk_worker.call(w, 0)] = [w, 0] }

done = {}
deaths = 0
until live.empty?
  begin
    r, v = Ractor.select(*live.keys)
    done[live.delete(r)[0]] = v
  rescue Ractor::RemoteError => e
    wid, att = live.delete(e.ractor)
    raise "wrong cause" unless e.cause.message.include?("thread blew up")
    deaths += 1
    GC.start
    live[mk_worker.call(wid, att + 1)] = [wid, att + 1]
  end
end

raise "FAIL deaths #{deaths}" unless deaths == 1
N_WORKERS.times do |w|
  exp = 2.times.sum { |h| (BATCH / 2).times.sum { |i| w * 10 + h + i } }
  raise "FAIL w#{w}: #{done[w]} != #{exp}" unless done[w] == exp
end
puts "OK mix_thread_worker_death"
