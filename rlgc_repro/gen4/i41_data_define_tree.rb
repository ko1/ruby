# Data.define 製ツリーを make_shareable、reader が再帰 sum
# axes: depth=5 width=3 readers=8 compacts=10 data
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
DNode = Data.define(:v, :kids)
def build_dtree(d, w)
  kids = d.zero? ? [] : Array.new(w) { build_dtree(d - 1, w) }
  DNode.new(v: d, kids: kids.freeze)
end
def dtree_sum(n)
  n.v + n.kids.sum { |k| dtree_sum(k) }
end
TREE = Ractor.make_shareable(build_dtree(5, 3))
EXP = dtree_sum(TREE)
rs = 8.times.map do |rid|
  Ractor.new(TREE, rid) do |t, id|
    acc = 0
    5.times { acc += dtree_sum(t) }
    acc
  end
end
10.times { GC.compact }
rs.each { |ra| raise "mismatch" unless ra.value == EXP * 5 }

puts "OK i41_data_define_tree"
