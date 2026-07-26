# Weak ref semantics preserved across GC.compact
# axes: weakref semantics, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
class WRef
  def initialize(o); @m = ObjectSpace::WeakMap.new; @m[:o] = o; end
  def get; @m[:o]; end
  def alive?; @m.key?(:o); end
end
live = Object.new
w1 = WRef.new(live)
tmp = Object.new
w2 = WRef.new(tmp)
tmp = nil
GC.start
GC.compact
raise 'live ref must stay alive' unless w1.alive? && w1.get.equal?(live)
raise 'dropped ref should be collectable' if w2.alive?
puts "OK j50_weakref_compact"
