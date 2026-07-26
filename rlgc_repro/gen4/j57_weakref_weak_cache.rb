# Weak-value cache: pinned values survive, unpinned may be evicted by GC
# axes: weak cache, 0 ractors
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
class WRef
  def initialize(o); @m = ObjectSpace::WeakMap.new; @m[:o] = o; end
  def get; @m[:o]; end
  def alive?; @m.key?(:o); end
end
# a weak cache: keys strong, values weak; only pinned values survive
cache = ObjectSpace::WeakMap.new
pins = {}
200.times do |i|
  key = i
  val = "payload#{i}"
  cache[key] = val
  pins[key] = val if i % 4 == 0
end
GC.start
pins.each_key { |k| raise "evicted pinned #{k}" unless cache.key?(k) }
present = (0...200).count { |i| cache.key?(i) }
raise unless present >= pins.size && present <= 200
raise unless pins.size == 50
puts "OK j57_weakref_weak_cache"
