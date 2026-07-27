# f04 shared-config service: make_shareable depth-5 graph, reference sharing via object_id
# axes: shareable pass-by-reference, 3 readers, GC.compact mid-flow
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

def cfg(depth)
  return { key: "leaf#{depth}", w: 3 } if depth == 0
  { name: "n#{depth}", subs: [cfg(depth - 1), cfg(depth - 1)] }
end

graph = Ractor.make_shareable(cfg(5))
assert Ractor.shareable?(graph), "graph shareable"
gid = graph.object_id

port = Ractor::Port.new
3.times do |wi|
  Ractor.new(port, graph, wi) do |po, gg, myid|
    total = 0
    walker = lambda { |nn| nn.key?(:subs) ? nn[:subs].each { |ss| walker.call(ss) } : total += nn[:w] }
    walker.call(gg)
    po.send([myid, gg.object_id, total, gg.frozen?])
  end
end

GC.compact
3.times do
  wid, oid, total, fz = port.receive
  assert oid == gid, "reader #{wid}: reference not shared (#{oid} != #{gid})"
  assert fz, "reader #{wid}: not frozen"
  assert total == 32 * 3, "reader #{wid}: leaf sum #{total}"
end
puts "OK f04_shareable_graph_refs"
