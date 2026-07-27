# gen4 map-reduce: group-by over a shared frozen table of Struct records.
# Workers histogram by region; partial hashes merged and cross-checked in main.
# axes: transfer=copy, shared frozen Struct records, GC=none, exceptions=none
Event = Struct.new(:id, :region, :amount)
TABLE = Ractor.make_shareable(
  Array.new(900) { |i| Event.new(i, "region-#{i % 7}", (i * 13) % 100) }
)

N_WORKERS = 3

chunks = (0...TABLE.size).each_slice(TABLE.size / N_WORKERS).to_a
workers = chunks.map do |idxs|
  Ractor.new(idxs.first, idxs.last) do |lo, hi|
    hist = Hash.new { |h, k| h[k] = [0, 0] } # region -> [count, amount_sum]
    (lo..hi).each do |i|
      ev = TABLE[i]
      cell = hist[ev.region]
      cell[0] += 1
      cell[1] += ev.amount
    end
    hist.default_proc = nil
    hist
  end
end

merged = Hash.new { |h, k| h[k] = [0, 0] }
workers.each do |w|
  w.value.each do |region, (c, s)|
    merged[region][0] += c
    merged[region][1] += s
  end
end

raise "FAIL regions" unless merged.keys.sort == (0..6).map { |r| "region-#{r}" }
raise "FAIL count" unless merged.values.sum(&:first) == TABLE.size
raise "FAIL amount" unless merged.values.sum(&:last) == TABLE.sum(&:amount)
puts "OK mr_histogram_struct"
