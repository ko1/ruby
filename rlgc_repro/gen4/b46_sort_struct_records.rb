# Struct レコードを (score desc, id asc) で並列 sort し参照実装と比較
# axes: 2 workers, Struct payload, copy, merge in main
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

SRec = Struct.new(:id, :score)
N = 28
recs = Array.new(N) { |i| SRec.new(i, (i * 43) % 19) }
exp = recs.sort_by { |r| [-r.score, r.id] }.map { |r| [r.id, r.score] }
half = N / 2
rs = 2.times.map do |k|
  Ractor.new(recs[k * half, half]) { |a| a.sort_by { |r| [-r.score, r.id] } }
end
l, r = rs.map(&:value)
merged = []
i = j = 0
cmp = ->(x, y) { [-x.score, x.id] <=> [-y.score, y.id] }
while i < l.size && j < r.size
  if cmp.call(l[i], r[j]) <= 0
    merged << l[i]
    i += 1
  else
    merged << r[j]
    j += 1
  end
end
merged.concat(l[i..])
merged.concat(r[j..])
raise "sorted" unless merged.map { |x| [x.id, x.score] } == exp
puts "OK b46_sort_struct_records"
