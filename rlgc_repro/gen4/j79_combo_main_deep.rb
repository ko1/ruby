# Deep main-Ractor finalizer+weakmap rounds
# axes: finalizer+weakmap, main ractor, deep rounds
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
survived = 0
4.times do |r|
  wm = ObjectSpace::WeakMap.new
  keep = []
  30.times do |i|
    o = Object.new
    ObjectSpace.define_finalizer(o, proc { })
    wm[i] = o
    keep << o if i % 5 == 0
  end
  unless ENV['S_STRESS']
    GC.start
    GC.compact if r.even?
  end
  live = (0...30).select { |i| i % 5 == 0 }
  raise unless live.all? { |i| wm.key?(i) }
  survived += keep.size
end
exp = (0...30).count { |i| i % 5 == 0 } * 4
raise unless survived == exp
puts "OK j79_combo_main_deep"
