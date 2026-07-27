# gen4 supervisor: workers send periodic heartbeats to a status port while
# crunching; supervisor tracks liveness and reconciles heartbeat counts
# against per-worker results. One worker dies mid-run and is respawned.
# axes: transfer=copy, GC=none, exceptions=one worker death, payload=small
N_WORKERS = 5
UNITS = 50   # work units per worker; heartbeat every 10

status = Ractor::Port.new

mk_worker = lambda do |wid, attempt|
  Ractor.new(status, wid, attempt) do |st, id, att|
    Thread.current.report_on_exception = false
    sum = 0
    UNITS.times do |u|
      raise "collapse" if id == 2 && att == 0 && u == 25
      sum += id * 1000 + u
      st << [:beat, id, att, u] if (u + 1) % 10 == 0
    end
    [id, att, sum]
  end
end

live = {}
N_WORKERS.times { |w| live[mk_worker.call(w, 0)] = [w, 0] }

beats = Hash.new(0)
done = {}
deaths = 0
consumed = 0
until live.empty?
  begin
    which, msg = Ractor.select(status, *live.keys)
    if which == status
      tag, id, _att, _u = msg
      raise "bad status" unless tag == :beat
      beats[id] += 1
      consumed += 1
    else
      id, _att, sum = msg
      live.delete(which)
      done[id] = sum
    end
  rescue Ractor::RemoteError => e
    wid, att = live.delete(e.ractor)
    deaths += 1
    live[mk_worker.call(wid, att + 1)] = [wid, att + 1]
  end
end
# every beat was sent before its worker terminated, so the rest are buffered:
# 4 clean workers x5, dead worker attempt0 sent 2 (u=9,19), attempt1 sent 5.
total_beats = 4 * 5 + 2 + 5
(total_beats - consumed).times do
  tag, id, = status.receive
  raise "bad drain" unless tag == :beat
  beats[id] += 1
end

raise "FAIL deaths #{deaths}" unless deaths == 1
raise "FAIL done" unless done.size == N_WORKERS
N_WORKERS.times do |w|
  exp = UNITS.times.sum { |u| w * 1000 + u }
  raise "FAIL sum w#{w}" unless done[w] == exp
  exp_beats = w == 2 ? 7 : 5
  raise "FAIL beats w#{w}: #{beats[w]}" unless beats[w] == exp_beats
end
puts "OK sv_heartbeat"
