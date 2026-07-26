# each_object counts a large held set of Tag instances
# axes: each_object lower-bound, many
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
class Tag; end
held = 150.times.map { |i| Tag.new }
GC.start
c = ObjectSpace.each_object(Tag) { }
raise "count #{c} < #{held.__id__ && 150}" unless c >= 150
raise unless held.size == 150
puts "OK j42_each_object_many"
