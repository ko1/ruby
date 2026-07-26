# ivar を持つ通常オブジェクトで cyclic graph を作り make_shareable
# axes: verts=40 readers=4 compacts=6 ivars cyclic
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
class Vertex
  def initialize(id, w)
    @id = id
    @weight = w
    @edges = []
  end
  attr_reader :id, :weight, :edges
  def add(e); @edges << e; end
end
CNT = 40
verts = Array.new(CNT) { |i| Vertex.new(i, i + 1) }
verts.each_with_index do |vx, i|
  vx.add(verts[(i + 1) % CNT])
  vx.add(verts[(i * 2) % CNT])
  vx.instance_variable_get(:@edges).freeze
end
GRAPH = Ractor.make_shareable(verts.freeze)
EXP = (1..CNT).sum
rs = 4.times.map do |rid|
  Ractor.new(GRAPH, rid) do |g, id|
    acc = 0
    g.each { |vx| acc += vx.weight; vx.edges.each { |e| e.id } }
    acc
  end
end
6.times { GC.compact }
rs.each { |ra| raise "mismatch" unless ra.value == EXP }

puts "OK i60_frozen_ivar_object_graph"
