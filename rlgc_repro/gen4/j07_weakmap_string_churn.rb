# WeakMap string churn: many transient values, few retained
# axes: weakmap membership, string churn, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
wm = ObjectSpace::WeakMap.new
kept = []
150.times { |i| v = "s#{i}"*3; wm[i] = v; kept << v if i % 7 == 0 }
GC.start
GC.compact
expect = (0...150).select { |i| i % 7 == 0 }
expect.each { |i| raise "lost live weak value #{i}" unless wm.key?(i) }
sz = (0...150).count { |i| wm.key?(i) }
raise "too many #{sz}" unless sz >= expect.size && sz <= 150
kept.clear
puts "OK j07_weakmap_string_churn"
