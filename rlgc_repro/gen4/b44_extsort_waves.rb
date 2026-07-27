# external sort 風: wave ごとに chunk sort worker を respawn し、最後に全体 merge 検証
# axes: 2 waves x 2 workers respawn, copy, GC.compact between waves
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

all = []
runs = []
2.times do |w|
  vals = Array.new(24) { |i| (i * 89 + w * 17) % 173 }
  all.concat(vals)
  rs = 2.times.map { |k| Ractor.new(vals[k * 12, 12]) { |a| a.sort } }
  rs.each { |r| runs << r.value }
  GC.compact if w == 0
end
runs.each { |r| raise "run unsorted" unless r == r.sort }
merged = runs.flatten.sort
raise "merged" unless merged == all.sort
raise "len" unless merged.size == 48
puts "OK b44_extsort_waves"
