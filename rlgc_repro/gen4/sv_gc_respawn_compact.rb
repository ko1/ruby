# gen4 supervisor: heavy GC around the death/respawn seam — GC.compact after
# every respawn and GC.start inside dying workers just before they raise
# (dying heap holds fresh garbage while partial results are already ported out).
# axes: transfer=copy, GC=GC.compact at respawn + GC.start pre-death, exceptions=death+respawn
N_CHUNKS = 8
CHUNK = 25

results = Ractor::Port.new

mk_worker = lambda do |cid, attempt|
  Ractor.new(results, cid, attempt) do |res, c, att|
    Thread.current.report_on_exception = false
    scratch = []
    CHUNK.times do |i|
      idx = c * CHUNK + i
      scratch << "s#{idx}" * 5
      res << [c, idx] if i.even?
      if att == 0 && c.odd? && i == 15
        GC.start   # churn dying heap right before abort
        raise "die #{c}"
      end
    end
    [c, scratch.size]
  end
end

live = {}
N_CHUNKS.times { |c| live[mk_worker.call(c, 0)] = [c, 0] }

partials = Hash.new(0)
finished = {}
deaths = 0
consumed = 0
until live.empty?
  begin
    which, msg = Ractor.select(results, *live.keys)
    if which == results
      partials[msg[0]] += 1
      consumed += 1
    else
      live.delete(which)
      finished[msg[0]] = msg[1]
    end
  rescue Ractor::RemoteError => e
    cid, att = live.delete(e.ractor)
    deaths += 1
    GC.compact
    live[mk_worker.call(cid, att + 1)] = [cid, att + 1]
  end
end
# all remaining partials were sent before their sender terminated -> buffered
exp_deaths = (0...N_CHUNKS).count(&:odd?)
exp_per_chunk = (0...CHUNK).count(&:even?)      # full run partials
exp_dead_partial = (0..15).count(&:even?)       # partials before dying at i==15
exp_total = N_CHUNKS * exp_per_chunk + exp_deaths * exp_dead_partial
(exp_total - consumed).times { partials[results.receive[0]] += 1 }

raise "FAIL deaths" unless deaths == exp_deaths
raise "FAIL finished" unless finished.size == N_CHUNKS && finished.values.all? { |v| v == CHUNK }
got_total = partials.values.sum
raise "FAIL partials #{got_total} != #{exp_total}" unless got_total == exp_total
puts "OK sv_gc_respawn_compact"
