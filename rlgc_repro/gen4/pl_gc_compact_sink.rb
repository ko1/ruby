# gen4 pipeline: sink retains a window of processed items and runs GC.compact
# periodically while upstream stages keep sending; main also compacts once mid-run.
# axes: transfer=copy, GC=GC.compact in sink + main, exceptions=none, payload=nested hashes
N_ITEMS = 350

out = Ractor::Port.new

sink = Ractor.new(out) do |o|
  window = []
  n = sum = 0
  while (m = Ractor.receive) != :eos
    window << m
    window.shift if window.size > 25
    n += 1
    sum += m[:agg]
    GC.compact if n % 90 == 0
  end
  # window still live across the last compact
  GC.compact
  o << [n, sum, window.size]
end

norm = Ractor.new(sink) do |nxt|
  while (m = Ractor.receive) != :eos
    nxt << { id: m[:id], agg: m[:inner][:vals].sum + m[:inner][:tag].size }
  end
  nxt << :eos
end

expected = 0
N_ITEMS.times do |i|
  vals = [i, i % 6]
  tag = "tag#{i % 8}"
  expected += vals.sum + tag.size
  norm << { id: i, inner: { vals: vals, tag: tag } }
  GC.compact if i == 175
end
norm << :eos

n, sum, wsize = out.receive
[norm, sink].each(&:join)
raise "FAIL n #{n}" unless n == N_ITEMS
raise "FAIL sum #{sum} != #{expected}" unless sum == expected
raise "FAIL window #{wsize}" unless wsize == 25
puts "OK pl_gc_compact_sink"
