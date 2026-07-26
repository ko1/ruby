# Data.define 製ツリーを make_shareable、reader が再帰 sum
# axes: depth=7 width=2 readers=6 compacts=4 data
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
TREE = Ractor.make_shareable(build_dtree(7, 2))
EXP = dtree_sum(TREE)
rs = 6.times.map do |rid|
  Ractor.new(TREE, rid) do |t, id|
    acc = 0
    5.times { acc += dtree_sum(t) }
    acc
  end
end
4.times { GC.compact }
rs.each { |ra| raise "mismatch" unless ra.value == EXP * 5 }

puts "OK i40_data_define_tree"
