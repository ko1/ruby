# f48 index service: Hash keyed by symbols/ints/bignums/floats/ranges/true/nil, copy + remote lookup
# axes: copy, exotic hash keys, rehash on receive, GC.start
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

port = Ractor::Port.new
w = Ractor.new(port) do |po|
  mm = Ractor.receive
  probes = [:name, 7, 2**99, 2.75, (1..3), true, nil]
  po.send([probes.map { |kk| mm[kk] }, mm.size])
end

idx = {
  name: "leaf-sym",
  7 => "leaf-int",
  2**99 => "leaf-big",
  2.75 => "leaf-float",
  (1..3) => "leaf-range",
  true => "leaf-true",
  nil => "leaf-nil",
}
w.send(idx)
GC.start
vals, sz = port.receive
assert vals == ["leaf-sym", "leaf-int", "leaf-big", "leaf-float", "leaf-range", "leaf-true", "leaf-nil"],
       "remote lookups #{vals.inspect}"
assert sz == 7, "size"
assert idx[2**99] == "leaf-big", "source bignum key still works"
puts "OK f48_leaf_key_hash"
