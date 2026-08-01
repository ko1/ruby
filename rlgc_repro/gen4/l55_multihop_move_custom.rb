# payload moved A->B->C across three Ractors; compact mid-flight
# axes: move, 3 hops, compact, custom
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
class Cx55
  attr_accessor :v
  def initialize(v); @v = v; end
  def marshal_dump; @v; end
  def marshal_load(v); @v = v; end
end
port = Ractor::Port.new
c = Ractor.new(port) { |o| x = Ractor.receive; o.send([x.class.name, x.v]) }
bx = Ractor.new(c) { |dst| x = Ractor.receive; x.v += 1; dst.send(x, move: true) }
x = Cx55.new(41)
bx.send(x, move: true)
GC.compact
cls, v = port.receive; bx.value; c.value
raise unless cls == "Cx55" && v == 42
puts "OK l55_multihop_move_custom"
