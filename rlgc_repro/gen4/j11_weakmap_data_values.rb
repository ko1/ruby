# WeakMap holding Data instances; survivors deterministic
# axes: weakmap membership, data values
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
Rec = Data.define(:n)
wm = ObjectSpace::WeakMap.new
kept = []
84.times { |i| v = Rec.new(i); wm[i] = v; kept << v if i % 4 == 0 }
GC.start
expect = (0...84).select { |i| i % 4 == 0 }
expect.each { |i| raise "lost live weak value #{i}" unless wm.key?(i) }
sz = (0...84).count { |i| wm.key?(i) }
raise "too many #{sz}" unless sz >= expect.size && sz <= 84
kept.clear
puts "OK j11_weakmap_data_values"
