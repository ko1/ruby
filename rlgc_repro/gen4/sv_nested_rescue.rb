# gen4 supervisor: workers rescue "soft" errors internally but die on "hard"
# ones; supervisor only sees hard deaths and respawns. Mixed error taxonomy.
# axes: transfer=copy, GC=none, exceptions=rescued-in-worker AND worker death
class SoftError < StandardError; end
class HardError < StandardError; end

N_CHUNKS = 10
CHUNK = 30

mk_worker = lambda do |cid, attempt|
  Ractor.new(cid, attempt) do |c, att|
    Thread.current.report_on_exception = false
    soft = 0
    sum = 0
    CHUNK.times do |i|
      idx = c * CHUNK + i
      begin
        raise SoftError, "soft #{idx}" if idx % 7 == 2
        raise HardError, "hard #{idx}" if att == 0 && c % 3 == 1 && i == 20
        sum += idx
      rescue SoftError
        soft += 1
      end
    end
    [c, sum, soft]
  end
end

live = {}
N_CHUNKS.times { |c| live[mk_worker.call(c, 0)] = [c, 0] }

done = {}
hard_deaths = 0
until live.empty?
  begin
    r, (cid, sum, soft) = Ractor.select(*live.keys)
    live.delete(r)
    done[cid] = [sum, soft]
  rescue Ractor::RemoteError => e
    cid, att = live.delete(e.ractor)
    raise "wrong error" unless e.cause.is_a?(HardError)
    hard_deaths += 1
    live[mk_worker.call(cid, att + 1)] = [cid, att + 1]
  end
end

exp_deaths = (0...N_CHUNKS).count { |c| c % 3 == 1 }
raise "FAIL deaths #{hard_deaths}" unless hard_deaths == exp_deaths
raise "FAIL chunks" unless done.size == N_CHUNKS
all = (0...(N_CHUNKS * CHUNK))
exp_soft = all.count { |i| i % 7 == 2 }
exp_sum = all.sum - all.select { |i| i % 7 == 2 }.sum
raise "FAIL soft" unless done.values.sum(&:last) == exp_soft
raise "FAIL sum" unless done.values.sum(&:first) == exp_sum
puts "OK sv_nested_rescue"
