# Large WeakMap churn with sparse retention and compaction
# axes: weakmap membership, large churn, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
wm = ObjectSpace::WeakMap.new
kept = []
200.times { |i| v = "big#{i}"; wm[i] = v; kept << v if i % 9 == 0 }
GC.start
GC.compact
expect = (0...200).select { |i| i % 9 == 0 }
expect.each { |i| raise "lost live weak value #{i}" unless wm.key?(i) }
sz = (0...200).count { |i| wm.key?(i) }
raise "too many #{sz}" unless sz >= expect.size && sz <= 200
kept.clear
puts "OK j10_weakmap_big_churn"
