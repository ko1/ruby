# nested custom Marshal objects moved; compact
# axes: move, Marshal nested, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
class Node74
  attr_reader :a, :b
  def initialize(a, b); @a = a; @b = b; end
  def marshal_dump; [@a, @b]; end
  def marshal_load(m); @a, @b = m; end
end
port = Ractor::Port.new
w = Ractor.new(port) do |o|
  n = Ractor.receive
  o.send([n.class.name, n.a[:list].sum])
end
payload = Node74.new({ list: (1..80).to_a }, 7)
w.send(payload, move: true)
GC.compact
cls, val = port.receive; w.value
raise "cls #{cls}" unless cls == "Node74"
raise unless val == (1..80).sum
puts "OK l74_marshal_nested_custom_move"
