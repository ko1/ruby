# custom class via Marshal fallback moved
# axes: move, Marshal fallback, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
class Cust38
  attr_reader :v
  def initialize(v); @v = v; end
  def marshal_dump; @v; end
  def marshal_load(v); @v = v; end
end
port = Ractor::Port.new
w = Ractor.new(port) { |o| c = Ractor.receive; o.send([c.class.name, c.v.sum]) }
c = Cust38.new((1..120).to_a)
w.send(c, move: true)
GC.compact
cls, val = port.receive; w.value
raise "cls #{cls}" unless cls == "Cust38"
raise unless val == (1..120).sum
puts "OK l38_marshal_custom_array_move"
