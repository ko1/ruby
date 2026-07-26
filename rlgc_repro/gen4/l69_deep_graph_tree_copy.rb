# deep/wide object graph copied; compact during window
# axes: copy, deep graph, compact
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
def build69(depth)
  return { val: depth, kids: [] } if depth == 0
  { val: depth, kids: Array.new(2) { build69(depth - 1) } }
end
root = build69(8)
exp = (2**9) - 1
w.send(root, move: false)
GC.compact
c = port.receive; w.value
raise "tree #{c}!=#{exp}" unless c == exp
puts "OK l69_deep_graph_tree_copy"
