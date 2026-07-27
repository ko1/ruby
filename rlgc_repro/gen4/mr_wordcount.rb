# gen4 map-reduce: word count. Deep-frozen shareable corpus read directly by
# mappers via a constant; chunks are index ranges; partial hashes merged in main.
# axes: transfer=copy (results), shared frozen input, GC=none, exceptions=none
WORDS = %w[ruby ractor heap page mark sweep compact barrier port move copy].freeze
CORPUS = Ractor.make_shareable(
  Array.new(600) { |i| Array.new(6) { |j| WORDS[(i * 7 + j * 3) % WORDS.size] }.join(" ") }
)

N_WORKERS = 5

chunks = (0...CORPUS.size).each_slice(CORPUS.size / N_WORKERS + 1).to_a
mappers = chunks.map do |idxs|
  Ractor.new(idxs.first, idxs.last) do |lo, hi|
    counts = Hash.new(0)
    (lo..hi).each do |i|
      CORPUS[i].split.each { |w| counts[w] += 1 }
    end
    counts
  end
end

merged = Hash.new(0)
mappers.each { |m| m.value.each { |w, c| merged[w] += c } }

expected = Hash.new(0)
CORPUS.each { |line| line.split.each { |w| expected[w] += 1 } }
raise "FAIL total" unless merged.values.sum == CORPUS.size * 6
raise "FAIL merge #{merged}" unless merged == expected
puts "OK mr_wordcount"
