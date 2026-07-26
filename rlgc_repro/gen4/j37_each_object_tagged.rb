# each_object (main) counts at least the held Tag instances after GC
# axes: each_object lower-bound, 0 ractors
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
class Tag; end
held = 50.times.map { |i| Tag.new }
GC.start
c = ObjectSpace.each_object(Tag) { }
raise "count #{c} < #{held.__id__ && 50}" unless c >= 50
raise unless held.size == 50
puts "OK j37_each_object_tagged"
