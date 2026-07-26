# each_object main lower bound with a worker doing local GC
# axes: each_object lower-bound, 1 ractor, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
class Tag; end
held = 45.times.map { |i| Tag.new }
GC.start
GC.compact
port = Ractor::Port.new
w = Ractor.new(port) do |p|
  local = 30.times.map { Tag.new }
  GC.start
  p.send(local.size)
  local.size
end
raise unless port.receive == 30
raise unless w.value == 30
c = ObjectSpace.each_object(Tag) { }
raise "count #{c} < #{held.__id__ && 45}" unless c >= 45
raise unless held.size == 45
puts "OK j41_each_object_worker_compact"
