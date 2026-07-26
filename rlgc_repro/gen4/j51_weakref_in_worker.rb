# Weak ref semantics inside a worker Ractor (local GC)
# axes: weakref semantics, 1 ractor
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
class WRef
  def initialize(o); @m = ObjectSpace::WeakMap.new; @m[:o] = o; end
  def get; @m[:o]; end
  def alive?; @m.key?(:o); end
end
port = Ractor::Port.new
r = Ractor.new(port) do |p|
  live = Object.new
  w1 = WRef.new(live)
  tmp = Object.new
  w2 = WRef.new(tmp)
  tmp = nil
  GC.start
  raise 'live ref must stay alive' unless w1.alive? && w1.get.equal?(live)
  raise 'dropped ref should be collectable' if w2.alive?
  p.send(:ok)
  :ok
end
raise unless port.receive == :ok
raise unless r.value == :ok
puts "OK j51_weakref_in_worker"
