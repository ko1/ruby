# custom class via Marshal fallback moved
# axes: move, Marshal fallback, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
class Cust35
  attr_reader :v
  def initialize(v); @v = v; end
  def marshal_dump; @v; end
  def marshal_load(v); @v = v; end
end
port = Ractor::Port.new
w = Ractor.new(port) { |o| c = Ractor.receive; o.send([c.class.name, c.v]) }
c = Cust35.new(4242)
w.send(c, move: true)
GC.compact
cls, val = port.receive; w.value
raise "cls #{cls}" unless cls == "Cust35"
raise unless val == 4242
puts "OK l35_marshal_custom_scalar_move"
