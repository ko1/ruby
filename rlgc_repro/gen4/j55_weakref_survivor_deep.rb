# Deep per-Ractor weak-ref churn with sparse retention
# axes: weakref set, 2 ractors, deep
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
class WRef
  def initialize(o); @m = ObjectSpace::WeakMap.new; @m[:o] = o; end
  def get; @m[:o]; end
  def alive?; @m.key?(:o); end
end
port = Ractor::Port.new
ws = 2.times.map do |rid|
  Ractor.new(port, rid) do |p, id|
    refs = []
    keep = []
    150.times do |i|
      o = Object.new
      refs << WRef.new(o)
      keep << o if i % 7 == 0
    end
    GC.start
    alive = refs.count(&:alive?)
    want = (0...150).count { |i| i % 7 == 0 }
    p.send(alive >= want)
    want
  end
end
2.times { raise unless port.receive == true }
exp = (0...150).count { |i| i % 7 == 0 }
raise unless ws.map(&:value).all? { |v| v == exp }
puts "OK j55_weakref_survivor_deep"
