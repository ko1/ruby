# Six Ractors each with weak-ref survivor sets
# axes: weakref set, 6 ractors
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
class WRef
  def initialize(o); @m = ObjectSpace::WeakMap.new; @m[:o] = o; end
  def get; @m[:o]; end
  def alive?; @m.key?(:o); end
end
port = Ractor::Port.new
ws = 6.times.map do |rid|
  Ractor.new(port, rid) do |p, id|
    refs = []
    keep = []
    55.times do |i|
      o = Object.new
      refs << WRef.new(o)
      keep << o if i % 5 == 0
    end
    GC.start
    alive = refs.count(&:alive?)
    want = (0...55).count { |i| i % 5 == 0 }
    p.send(alive >= want)
    want
  end
end
6.times { raise unless port.receive == true }
exp = (0...55).count { |i| i % 5 == 0 }
raise unless ws.map(&:value).all? { |v| v == exp }
puts "OK j56_weakref_survivor_x6"
