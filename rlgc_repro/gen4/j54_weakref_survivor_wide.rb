# Wide fan of weak-ref survivor sets across many Ractors
# axes: weakref set, 8 ractors
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
class WRef
  def initialize(o); @m = ObjectSpace::WeakMap.new; @m[:o] = o; end
  def get; @m[:o]; end
  def alive?; @m.key?(:o); end
end
port = Ractor::Port.new
ws = 8.times.map do |rid|
  Ractor.new(port, rid) do |p, id|
    refs = []
    keep = []
    40.times do |i|
      o = Object.new
      refs << WRef.new(o)
      keep << o if i % 4 == 0
    end
    GC.start
    alive = refs.count(&:alive?)
    want = (0...40).count { |i| i % 4 == 0 }
    p.send(alive >= want)
    want
  end
end
8.times { raise unless port.receive == true }
exp = (0...40).count { |i| i % 4 == 0 }
raise unless ws.map(&:value).all? { |v| v == exp }
puts "OK j54_weakref_survivor_wide"
