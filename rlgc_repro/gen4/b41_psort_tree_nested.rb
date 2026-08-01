# merge sort を ractor ツリーで: 親 ractor が子 2 つを spawn して merge (深さ 2)
# axes: nested ractors (3 spawn), value join in parent
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 40
arr = Array.new(N) { |i| (i * 61 + 7) % 151 }
exp = arr.sort

root = Ractor.new(arr) do |a|
  half = a.size / 2
  kids = 2.times.map { |k| Ractor.new(a[k * half, half]) { |p| p.sort } }
  l, r = kids.map(&:value)
  out = []
  i = j = 0
  while i < l.size && j < r.size
    if l[i] <= r[j]
      out << l[i]
      i += 1
    else
      out << r[j]
      j += 1
    end
  end
  out.concat(l[i..])
  out.concat(r[j..])
  out
end
got = root.value
raise "sorted" unless got == exp
puts "OK b41_psort_tree_nested"
