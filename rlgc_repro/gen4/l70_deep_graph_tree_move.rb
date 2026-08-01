# deep/wide object graph moved; compact during window
# axes: move, deep graph, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
w = Ractor.new(port) do |o|
  root = Ractor.receive
  count = 0
  stack = [root]
  until stack.empty?
    n = stack.pop; count += 1
    stack.concat(n[:kids])
  end
  o.send(count)
end
def build70(depth)
  return { val: depth, kids: [] } if depth == 0
  { val: depth, kids: Array.new(2) { build70(depth - 1) } }
end
root = build70(8)
exp = (2**9) - 1
w.send(root, move: true)
GC.compact
c = port.receive; w.value
raise "tree #{c}!=#{exp}" unless c == exp
puts "OK l70_deep_graph_tree_move"
