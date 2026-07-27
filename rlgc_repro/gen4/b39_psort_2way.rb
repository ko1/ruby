# 2-way 並列 merge sort: 半分ずつ worker が sort、main が merge して Array#sort と比較
# axes: 2 workers, copy in / value out
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 60
arr = Array.new(N) { |i| (i * 173 + 41) % 251 }
exp = arr.sort
half = N / 2
rs = 2.times.map { |k| Ractor.new(arr[k * half, half]) { |a| a.sort } }
l, r = rs.map(&:value)
merged = []
i = j = 0
while i < l.size && j < r.size
  if l[i] <= r[j]
    merged << l[i]
    i += 1
  else
    merged << r[j]
    j += 1
  end
end
merged.concat(l[i..]) if i < l.size
merged.concat(r[j..]) if j < r.size
raise "sorted" unless merged == exp
puts "OK b39_psort_2way"
