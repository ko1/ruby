# WeakMap: live values held by strong refs survive GC, others may be collected
# axes: weakmap membership, GC survivors deterministic, 0 ractors
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
wm = ObjectSpace::WeakMap.new
kept = []
120.times { |i| v = "val#{i}"; wm[i] = v; kept << v if i % 3 == 0 }
GC.start
expect = (0...120).select { |i| i % 3 == 0 }
expect.each { |i| raise "lost live weak value #{i}" unless wm.key?(i) }
sz = (0...120).count { |i| wm.key?(i) }
raise "too many #{sz}" unless sz >= expect.size && sz <= 120
kept.clear
puts "OK j01_weakmap_kept_survivors"
