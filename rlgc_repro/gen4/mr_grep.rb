# gen4 map-reduce: parallel grep over a shared frozen line table. Workers
# build unshareable result arrays (matching line copies) returned via #value.
# axes: transfer=copy, shared frozen input + Regexp, GC=one GC.start per worker, exceptions=none
LINES = Ractor.make_shareable(
  Array.new(800) { |i| "#{i % 4 == 0 ? 'ERROR' : 'info'} event=#{i} node=n#{i % 5}" }
)
PATTERN = Ractor.make_shareable(/\AERROR .*node=n[0-2]\z/)

N_WORKERS = 4

chunks = (0...LINES.size).each_slice(LINES.size / N_WORKERS).to_a
workers = chunks.map do |idxs|
  Ractor.new(idxs.first, idxs.last) do |lo, hi|
    hits = []
    (lo..hi).each do |i|
      line = LINES[i]
      hits << line.dup if PATTERN.match?(line)
    end
    GC.start
    hits
  end
end

all = workers.flat_map(&:value)
expected = LINES.grep(PATTERN)
raise "FAIL count #{all.size} != #{expected.size}" unless all.size == expected.size
raise "FAIL content" unless all.sort == expected.sort
puts "OK mr_grep"
