# WeakMap with Hash values; kept entries survive repeated GC
# axes: weakmap membership, hash values, 0 ractors
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
wm = ObjectSpace::WeakMap.new
kept = []
110.times { |i| v = {k: i}; wm[i] = v; kept << v if i % 4 == 0 }
GC.start
expect = (0...110).select { |i| i % 4 == 0 }
expect.each { |i| raise "lost live weak value #{i}" unless wm.key?(i) }
sz = (0...110).count { |i| wm.key?(i) }
raise "too many #{sz}" unless sz >= expect.size && sz <= 110
kept.clear
puts "OK j06_weakmap_hash_values"
