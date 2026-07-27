# gen4 map-reduce: persistent worker pool reused across 6 waves. Each wave
# scatters [range, wave_mult] jobs to worker default ports; partials gathered
# via one result port; grand totals checked per wave.
# axes: transfer=copy, shared frozen input, GC=GC.start between waves, exceptions=none
NUMS = Ractor.make_shareable(Array.new(1200) { |i| (i * 17) % 101 })

N_WORKERS = 4
N_WAVES = 6

results = Ractor::Port.new
workers = N_WORKERS.times.map do |wid|
  Ractor.new(results, wid) do |res, id|
    waves = 0
    while (job = Ractor.receive) != :stop
      lo, hi, mult = job
      part = (lo..hi).sum { |i| NUMS[i] * mult }
      waves += 1
      res << [id, part]
    end
    waves
  end
end

slice = NUMS.size / N_WORKERS
N_WAVES.times do |wave|
  mult = wave + 1
  N_WORKERS.times do |w|
    lo = w * slice
    hi = w == N_WORKERS - 1 ? NUMS.size - 1 : lo + slice - 1
    workers[w] << [lo, hi, mult]
  end
  total = 0
  N_WORKERS.times { total += results.receive[1] }
  expected = NUMS.sum * mult
  raise "FAIL wave#{wave}: #{total} != #{expected}" unless total == expected
  GC.start if wave.odd?
end
workers.each { |w| w << :stop }
raise "FAIL waves" unless workers.sum(&:value) == N_WAVES * N_WORKERS
puts "OK mr_multiwave"
