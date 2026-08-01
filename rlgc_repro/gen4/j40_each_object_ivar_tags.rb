# each_object counts held tagged objects carrying ivars
# axes: each_object lower-bound, ivars, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
class Tag; def initialize(i); @i=i; end; end
held = 55.times.map { |i| Tag.new(i) }
GC.start
GC.compact
c = ObjectSpace.each_object(Tag) { }
raise "count #{c} < #{held.__id__ && 55}" unless c >= 55
raise unless held.size == 55
puts "OK j40_each_object_ivar_tags"
