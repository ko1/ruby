# Per-Ractor weak-ref sets: kept targets stay alive after local GC
# axes: weakref set, 4 ractors
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
class WRef
  def initialize(o); @m = ObjectSpace::WeakMap.new; @m[:o] = o; end
  def get; @m[:o]; end
  def alive?; @m.key?(:o); end
end
port = Ractor::Port.new
ws = 4.times.map do |rid|
  Ractor.new(port, rid) do |p, id|
    refs = []
    keep = []
    60.times do |i|
      o = Object.new
      refs << WRef.new(o)
      keep << o if i % 3 == 0
    end
    GC.start
    alive = refs.count(&:alive?)
    want = (0...60).count { |i| i % 3 == 0 }
    p.send(alive >= want)
    want
  end
end
4.times { raise unless port.receive == true }
exp = (0...60).count { |i| i % 3 == 0 }
raise unless ws.map(&:value).all? { |v| v == exp }
puts "OK j53_weakref_survivor_set"
