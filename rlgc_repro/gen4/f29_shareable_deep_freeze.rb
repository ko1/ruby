# f29 immutability auditor: make_shareable deep-freezes every level; worker walks and audits
# axes: shareable deep graph, frozen audit at all depths, GC.compact
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

def tree(depth)
  return [+"leaf#{depth}", { t: +"tag" }] if depth == 0
  { n: +"node#{depth}", kids: [tree(depth - 1), tree(depth - 1)] }
end

g = tree(5)
assert !g[:n].frozen?, "pre: mutable"
Ractor.make_shareable(g)
assert g[:n].frozen?, "post: root string frozen"

port = Ractor::Port.new
Ractor.new(port, g) do |po, gg|
  frozen_count = 0
  total = 0
  audit = lambda do |nn|
    total += 1
    frozen_count += 1 if nn.frozen?
    case nn
    when Hash then nn.each_value { |vv| audit.call(vv) }
    when Array then nn.each { |vv| audit.call(vv) }
    end
  end
  audit.call(gg)
  GC.compact
  po.send([frozen_count, total])
end
fc, tot = port.receive
assert fc == tot, "all #{tot} nodes frozen, got #{fc}"
assert tot > 100, "walked a real graph (#{tot})"
puts "OK f29_shareable_deep_freeze"
