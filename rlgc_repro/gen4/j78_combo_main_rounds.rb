# Main-Ractor rounds of finalizer+weakmap churn with alternating compaction
# axes: finalizer+weakmap, main ractor, rounds
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
survived = 0
5.times do |r|
  wm = ObjectSpace::WeakMap.new
  keep = []
  25.times do |i|
    o = Object.new
    ObjectSpace.define_finalizer(o, proc { })
    wm[i] = o
    keep << o if i % 5 == 0
  end
  unless ENV['S_STRESS']
    GC.start
    GC.compact if r.even?
  end
  live = (0...25).select { |i| i % 5 == 0 }
  raise unless live.all? { |i| wm.key?(i) }
  survived += keep.size
end
exp = (0...25).count { |i| i % 5 == 0 } * 5
raise unless survived == exp
puts "OK j78_combo_main_rounds"
