# 文字列を [長さ, 辞書順] キーで並列 sort し sort_by 参照と比較
# axes: 2 workers, copy, string keys
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 30
strs = Array.new(N) { |i| "s" * (i * 7 % 5 + 1) + "#{(i * 13) % 10}" }
exp = strs.sort_by { |s| [s.size, s] }
half = N / 2
rs = 2.times.map { |k| Ractor.new(strs[k * half, half]) { |a| a.sort_by { |s| [s.size, s] } } }
l, r = rs.map(&:value)
merged = []
i = j = 0
key = ->(s) { [s.size, s] }
while i < l.size && j < r.size
  if (key.call(l[i]) <=> key.call(r[j])) <= 0
    merged << l[i]
    i += 1
  else
    merged << r[j]
    j += 1
  end
end
merged.concat(l[i..])
merged.concat(r[j..])
raise "sorted" unless merged == exp
puts "OK b43_sort_strings_key"
