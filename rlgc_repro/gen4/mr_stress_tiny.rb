# gen4 map-reduce: tiny corpus; ONE mapper runs under GC.stress=true, the
# others run normally. Kept very small so the stressed mapper stays fast.
# axes: transfer=copy, GC=GC.stress in 1 ractor, exceptions=none, payload=small
CORPUS = Ractor.make_shareable(Array.new(60) { |i| "w#{i % 9} " * (1 + i % 4) })

N_WORKERS = 3

chunks = (0...CORPUS.size).each_slice(CORPUS.size / N_WORKERS).to_a
workers = chunks.each_with_index.map do |idxs, wid|
  Ractor.new(idxs.first, idxs.last, wid) do |lo, hi, id|
    GC.stress = true if id == 1
    counts = Hash.new(0)
    (lo..hi).each { |i| CORPUS[i].split.each { |w| counts[w] += 1 } }
    GC.stress = false if id == 1
    counts
  end
end

merged = Hash.new(0)
workers.each { |w| w.value.each { |k, c| merged[k] += c } }
expected = Hash.new(0)
CORPUS.each { |line| line.split.each { |w| expected[w] += 1 } }
raise "FAIL #{merged}" unless merged == expected
puts "OK mr_stress_tiny"
