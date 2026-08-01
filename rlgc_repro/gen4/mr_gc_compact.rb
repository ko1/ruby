# gen4 map-reduce: two waves; main runs GC.compact between scatter and gather
# while workers hold references into the shared frozen input; workers also
# compact once mid-chunk.
# axes: transfer=copy, GC=GC.compact (main + workers), exceptions=none
DATA = Ractor.make_shareable(Array.new(700) { |i| ["key#{i % 50}", "v" * (1 + i % 20), i] })

N_WORKERS = 4

run_wave = lambda do |mult|
  chunks = (0...DATA.size).each_slice(DATA.size / N_WORKERS + 1).to_a
  workers = chunks.map do |idxs|
    Ractor.new(idxs.first, idxs.last, mult) do |lo, hi, m|
      acc = Hash.new(0)
      held = []
      (lo..hi).each do |i|
        k, v, num = DATA[i]
        held << v            # keep refs to shared strings across compaction
        acc[k] += num * m + v.size
        GC.compact if i == lo + 40
      end
      raise "held" unless held.all? { |s| s.start_with?("v") }
      acc
    end
  end
  GC.compact
  merged = Hash.new(0)
  workers.each { |w| w.value.each { |k, v| merged[k] += v } }
  merged
end

w1 = run_wave.call(1)
GC.compact
w2 = run_wave.call(2)

exp1 = Hash.new(0)
DATA.each { |k, v, num| exp1[k] += num + v.size }
exp2 = Hash.new(0)
DATA.each { |k, v, num| exp2[k] += num * 2 + v.size }
raise "FAIL wave1" unless w1 == exp1
raise "FAIL wave2" unless w2 == exp2
puts "OK mr_gc_compact"
