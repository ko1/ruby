# f49 intern service: shareable leaves (symbol, made-shareable string, range of ints) keep identity across ractors
# axes: shareable identity via object_id, GC.compact stability
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

tag = Ractor.make_shareable(+"interned-tag")
rng = Ractor.make_shareable((1..1000))
ids = { sym: :interned_sym.object_id, tag: tag.object_id, rng: rng.object_id }

port = Ractor::Port.new
2.times do |wi|
  Ractor.new(port, tag, rng, wi) do |po, tt, rr, myid|
    GC.compact
    po.send([myid, :interned_sym.object_id, tt.object_id, rr.object_id, rr.sum])
  end
end
GC.compact
2.times do
  wid, sid, tid, rid, rsum = port.receive
  assert sid == ids[:sym], "reader #{wid}: symbol identity"
  assert tid == ids[:tag], "reader #{wid}: shareable string identity"
  assert rid == ids[:rng], "reader #{wid}: shareable range identity"
  assert rsum == 500_500, "reader #{wid}: range usable"
end
GC.compact
assert tag.object_id == ids[:tag], "object_id stable across compact"
puts "OK f49_frozen_leaf_identity"
