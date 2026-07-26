# nested custom Marshal objects copied; compact
# axes: copy, Marshal nested, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
class Node75
  attr_reader :a, :b
  def initialize(a, b); @a = a; @b = b; end
  def marshal_dump; [@a, @b]; end
  def marshal_load(m); @a, @b = m; end
end
port = Ractor::Port.new
w = Ractor.new(port) do |o|
  arr = Ractor.receive
  o.send([arr.first.class.name, arr.sum { |n| n.b }])
end
payload = Array.new(50) { |i| Node75.new(i, i * i) }
w.send(payload, move: false)
GC.compact
cls, val = port.receive; w.value
raise "cls #{cls}" unless cls == "Node75"
raise unless val == (0...50).map { |i| i * i }.sum
puts "OK l75_marshal_nested_custom_copy"
