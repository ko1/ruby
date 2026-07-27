# 4-way sort + main の k-way merge (先頭最小 chunk 選択)
# axes: 4 workers, copy, k-way merge
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 64
arr = Array.new(N) { |i| (i * 97 + 13) % 199 }
exp = arr.sort
qs = N / 4
rs = 4.times.map { |k| Ractor.new(arr[k * qs, qs]) { |a| a.sort } }
chunks = rs.map(&:value)
idx = [0, 0, 0, 0]
merged = []
N.times do
  best = nil
  bi = -1
  4.times do |c|
    next if idx[c] >= chunks[c].size
    v = chunks[c][idx[c]]
    if best.nil? || v < best
      best = v
      bi = c
    end
  end
  merged << best
  idx[bi] += 1
end
GC.start
raise "len" unless merged.size == N
raise "sorted" unless merged == exp
puts "OK b40_psort_4way_kmerge"
