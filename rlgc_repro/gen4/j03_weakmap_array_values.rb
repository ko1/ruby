# WeakMap with Array values; membership after GC.compact
# axes: weakmap membership, array values, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
wm = ObjectSpace::WeakMap.new
kept = []
90.times { |i| v = [i, i*i]; wm[i] = v; kept << v if i % 5 == 0 }
GC.start
GC.compact
expect = (0...90).select { |i| i % 5 == 0 }
expect.each { |i| raise "lost live weak value #{i}" unless wm.key?(i) }
sz = (0...90).count { |i| wm.key?(i) }
raise "too many #{sz}" unless sz >= expect.size && sz <= 90
kept.clear
puts "OK j03_weakmap_array_values"
