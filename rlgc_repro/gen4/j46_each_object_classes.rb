# each_object(Class) counts at least the held anonymous classes
# axes: each_object classes, lower-bound
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
klasses = 30.times.map { |i| Class.new }
GC.start
c = ObjectSpace.each_object(Class) { }
raise unless c >= 30
raise unless klasses.size == 30 && klasses.all? { |k| k.is_a?(Class) }
puts "OK j46_each_object_classes"
