# gen4 worker-pool: GC.compact sprinkled — main compacts between batches and
# one designated worker compacts every 30 jobs while holding live job state.
# axes: transfer=copy, GC=GC.compact (main + one worker), exceptions=none, payload=hash/arr
N_WORKERS = 4
N_JOBS = 240

results = Ractor::Port.new
workers = N_WORKERS.times.map do |wid|
  Ractor.new(results, wid) do |res, id|
    keep = []   # live state across compactions
    done = 0
    while (job = Ractor.receive) != :stop
      keep << job[:body].dup
      keep.shift if keep.size > 20
      done += 1
      GC.compact if id == 0 && done % 30 == 0
      res << [job[:seq], job[:body][:vals].sum]
    end
    [done, keep.sum { |h| h[:vals].sum }]
  end
end

expected = 0
N_JOBS.times do |i|
  vals = [i, i * 3, i % 11]
  expected += vals.sum
  workers[i % N_WORKERS] << { seq: i, body: { name: "job#{i}", vals: vals } }
  GC.compact if i % 60 == 59
end
workers.each { |w| w << :stop }

got = 0
N_JOBS.times { got += results.receive[1] }
GC.compact
totals = workers.map(&:value)
raise "FAIL done" unless totals.sum(&:first) == N_JOBS
raise "FAIL #{got} != #{expected}" unless got == expected
puts "OK wp_compact"
