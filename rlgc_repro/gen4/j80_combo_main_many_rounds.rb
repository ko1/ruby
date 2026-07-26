# Many short rounds of main-Ractor finalizer+weakmap churn
# axes: finalizer+weakmap, main ractor, many rounds
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
survived = 0
8.times do |r|
  wm = ObjectSpace::WeakMap.new
  keep = []
  20.times do |i|
    o = Object.new
    ObjectSpace.define_finalizer(o, proc { })
    wm[i] = o
    keep << o if i % 5 == 0
  end
  unless ENV['S_STRESS']
    GC.start
    GC.compact if r.even?
  end
  live = (0...20).select { |i| i % 5 == 0 }
  raise unless live.all? { |i| wm.key?(i) }
  survived += keep.size
end
exp = (0...20).count { |i| i % 5 == 0 } * 8
raise unless survived == exp
puts "OK j80_combo_main_many_rounds"
