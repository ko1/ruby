# gen4 service+churn: ractor churn with aggressive GC between waves —
# GC.start after every wave and GC.compact every other wave in main, while
# two persistent services keep long-lived state referencing wave results.
# axes: transfer=copy, GC=GC.start+GC.compact between waves, lifecycle=churn
WAVES = 8
PER_WAVE = 30

stats = Ractor.new do
  seen = []
  while (m = Ractor.receive) != :shutdown
    seen << m
    seen.shift if seen.size > 100
  end
  seen.size
end

archive = Ractor.new do
  kept = {}
  while (m = Ractor.receive) != :shutdown
    kept[m[:id]] = m[:blob]
  end
  [kept.size, kept.values.sum(&:size)]
end

exp_blob = 0
WAVES.times do |w|
  tasks = PER_WAVE.times.map do |t|
    tid = w * PER_WAVE + t
    Ractor.new(stats, archive, tid) do |st, ar, id|
      st << { id: id, note: "task#{id}" }
      blob = "blob#{id}" * (2 + id % 4)
      ar << { id: id, blob: blob }
      blob.size
    end
  end
  exp_blob += tasks.sum(&:value)
  GC.start
  GC.compact if w.odd?
end

stats << :shutdown
archive << :shutdown
raise "FAIL stats" unless stats.value == 100
n, blob_total = archive.value
raise "FAIL archive n" unless n == WAVES * PER_WAVE
raise "FAIL blob #{blob_total} != #{exp_blob}" unless blob_total == exp_blob
puts "OK churn_gc_waves"
