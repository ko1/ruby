# each_object on main counts held objects after a worker terminates
# axes: each_object lower-bound, 1 ractor, absorb
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
class Tag; end
held = 40.times.map { |i| Tag.new }
GC.start
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
raise "count #{c} < #{held.__id__ && 40}" unless c >= 40
raise unless held.size == 40
puts "OK j39_each_object_after_worker"
