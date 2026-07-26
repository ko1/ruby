# each_object(Module) counts at least the held anonymous modules
# axes: each_object modules, lower-bound
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
klasses = 35.times.map { |i| Module.new }
GC.start
GC.compact
c = ObjectSpace.each_object(Module) { }
raise unless c >= 35
raise unless klasses.size == 35 && klasses.all? { |k| k.is_a?(Module) }
puts "OK j48_each_object_modules"
