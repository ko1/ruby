# WeakMap holding Struct instances; survivors deterministic
# axes: weakmap membership, struct values
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
Point = Struct.new(:x, :y)
wm = ObjectSpace::WeakMap.new
kept = []
96.times { |i| v = Point.new(i,i); wm[i] = v; kept << v if i % 4 == 0 }
GC.start
expect = (0...96).select { |i| i % 4 == 0 }
expect.each { |i| raise "lost live weak value #{i}" unless wm.key?(i) }
sz = (0...96).count { |i| wm.key?(i) }
raise "too many #{sz}" unless sz >= expect.size && sz <= 96
kept.clear
puts "OK j08_weakmap_struct_values"
