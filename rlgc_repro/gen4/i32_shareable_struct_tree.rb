# Struct 製ツリーを make_shareable、reader が再帰 sum
# axes: depth=3 width=4 readers=3 compacts=5 struct
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
SNode = Struct.new(:v, :kids)
def build_stree(d, w)
  kids = d.zero? ? [] : Array.new(w) { build_stree(d - 1, w) }
  SNode.new(d, kids.freeze)
end
def stree_sum(n)
  n.v + n.kids.sum { |k| stree_sum(k) }
end
TREE = Ractor.make_shareable(build_stree(3, 4))
EXP = stree_sum(TREE)
rs = 3.times.map do |rid|
  Ractor.new(TREE, rid) do |t, id|
    acc = 0
    5.times { acc += stree_sum(t) }
    acc
  end
end
5.times { GC.compact }
rs.each { |ra| raise "mismatch" unless ra.value == EXP * 5 }

puts "OK i32_shareable_struct_tree"
