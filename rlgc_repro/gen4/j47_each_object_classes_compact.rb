# each_object(Class) lower bound stable across GC.compact
# axes: each_object classes, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
klasses = 40.times.map { |i| Class.new }
GC.start
GC.compact
c = ObjectSpace.each_object(Class) { }
raise unless c >= 40
raise unless klasses.size == 40 && klasses.all? { |k| k.is_a?(Class) }
puts "OK j47_each_object_classes_compact"
