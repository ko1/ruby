# each_object stable lower bound after GC.compact
# axes: each_object lower-bound, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
class Tag; end
held = 60.times.map { |i| Tag.new }
GC.start
GC.compact
c = ObjectSpace.each_object(Tag) { }
raise "count #{c} < #{held.__id__ && 60}" unless c >= 60
raise unless held.size == 60
puts "OK j38_each_object_compact"
