# custom class via Marshal fallback copied
# axes: copy, Marshal fallback, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
class Cust34
  attr_reader :v
  def initialize(v); @v = v; end
  def marshal_dump; @v; end
  def marshal_load(v); @v = v; end
end
port = Ractor::Port.new
w = Ractor.new(port) { |o| c = Ractor.receive; o.send([c.class.name, c.v]) }
c = Cust34.new(4242)
w.send(c, move: false)
GC.compact
cls, val = port.receive; w.value
raise "cls #{cls}" unless cls == "Cust34"
raise unless val == 4242
puts "OK l34_marshal_custom_scalar_copy"
