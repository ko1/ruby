# custom class via Marshal fallback copied
# axes: copy, Marshal fallback, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
class Cust37
  attr_reader :v
  def initialize(v); @v = v; end
  def marshal_dump; @v; end
  def marshal_load(v); @v = v; end
end
port = Ractor::Port.new
w = Ractor.new(port) { |o| c = Ractor.receive; o.send([c.class.name, c.v[:list].sum]) }
c = Cust37.new({ list: (1..100).to_a, tag: "x" })
w.send(c, move: false)
GC.compact
cls, val = port.receive; w.value
raise "cls #{cls}" unless cls == "Cust37"
raise unless val == (1..100).sum
puts "OK l37_marshal_custom_nested_copy"
