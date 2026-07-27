# gen4 supervisor: workers process chunks; on first attempt a chunk containing
# a poison index dies mid-chunk (transient failure). Supervisor detects the
# death via Ractor.select raising RemoteError and respawns with attempt+1,
# which succeeds. All work must complete exactly once per surviving attempt.
# axes: transfer=copy, GC=GC.start after each respawn, exceptions=worker death + respawn
N_CHUNKS = 12
CHUNK = 40

mk_worker = lambda do |chunk_id, attempt|
  Ractor.new(chunk_id, attempt) do |cid, att|
    Thread.current.report_on_exception = false
    sum = 0
    CHUNK.times do |i|
      idx = cid * CHUNK + i
      raise "poison #{idx}" if att == 0 && cid.even? && i == 17
      sum += idx
    end
    [cid, sum]
  end
end

live = {}   # Ractor => [chunk_id, attempt]
N_CHUNKS.times { |c| r = mk_worker.call(c, 0); live[r] = [c, 0] }

done = {}
respawns = 0
until live.empty?
  begin
    r, (cid, sum) = Ractor.select(*live.keys)
    live.delete(r)
    raise "dup chunk #{cid}" if done.key?(cid)
    done[cid] = sum
  rescue Ractor::RemoteError => e
    cid, att = live.delete(e.ractor)
    respawns += 1
    GC.start
    nr = mk_worker.call(cid, att + 1)
    live[nr] = [cid, att + 1]
  end
end

expected_respawns = (0...N_CHUNKS).count(&:even?)
raise "FAIL respawns #{respawns}" unless respawns == expected_respawns
raise "FAIL chunks" unless done.size == N_CHUNKS
total = done.values.sum
raise "FAIL total" unless total == (0...(N_CHUNKS * CHUNK)).sum
puts "OK sv_respawn_value"
