# WeakMap with plain Object values; kept subset survives GC
# axes: weakmap membership, object values, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
wm = ObjectSpace::WeakMap.new
kept = []
100.times { |i| v = Object.new; wm[i] = v; kept << v if i % 4 == 0 }
GC.start
GC.compact
expect = (0...100).select { |i| i % 4 == 0 }
expect.each { |i| raise "lost live weak value #{i}" unless wm.key?(i) }
sz = (0...100).count { |i| wm.key?(i) }
raise "too many #{sz}" unless sz >= expect.size && sz <= 100
kept.clear
puts "OK j02_weakmap_object_values"
