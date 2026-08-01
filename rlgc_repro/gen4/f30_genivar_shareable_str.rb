# f30 interned-label service: frozen String with generic ivar made shareable; identity + ivar readable remotely
# axes: make_shareable(String+genivar), pass-by-reference, GC.compact
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

label = +"label-main"
label.instance_variable_set(:@meta, [:x, 1, "deep"])
Ractor.make_shareable(label)
assert label.frozen? && Ractor.shareable?(label), "label shareable"
assert label.instance_variable_get(:@meta).frozen?, "ivar deep-frozen"
lid = label.object_id

port = Ractor::Port.new
2.times do |wi|
  Ractor.new(port, label, wi) do |po, ll, myid|
    GC.compact
    po.send([myid, ll.object_id, ll, ll.instance_variable_get(:@meta)])
  end
end
2.times do
  wid, oid, txt, meta = port.receive
  assert oid == lid, "reader #{wid}: not shared by reference"
  assert txt == "label-main", "reader #{wid}: content"
  assert meta == [:x, 1, "deep"], "reader #{wid}: generic ivar via reference"
end
GC.compact
assert label.object_id == lid, "object_id stable across compact"
puts "OK f30_genivar_shareable_str"
