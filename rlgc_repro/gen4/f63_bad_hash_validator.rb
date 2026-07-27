# f63 input validator: key whose #hash raises is rejected at insert; clean keys ship fine
# axes: custom #hash raising (rescue path), copy of surviving hash, GC.start
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

class PoisonKey
  def hash
    raise TypeError, "poisoned hash"
  end
  def eql?(_other)
    false
  end
end

class CleanKey
  attr_reader :id
  def initialize(id)
    @id = id
  end
  def hash
    id.hash
  end
  def eql?(other)
    other.is_a?(CleanKey) && other.id == id
  end
end

store = {}
rejected = 0
[[CleanKey.new(1), :a], [PoisonKey.new, :b], [CleanKey.new(2), :c], [PoisonKey.new, :d]].each do |kk, vv|
  begin
    store[kk] = vv
  rescue TypeError => err
    assert err.message == "poisoned hash", "unexpected: #{err.message}"
    rejected += 1
  end
end
assert rejected == 2, "rejected #{rejected}"
assert store.size == 2, "store size"

port = Ractor::Port.new
Ractor.new(port) do |po|
  mm = Ractor.receive
  po.send([mm[CleanKey.new(1)], mm[CleanKey.new(2)], mm.size])
end.send(store)
GC.start
v1, v2, sz = port.receive
assert v1 == :a && v2 == :c && sz == 2, "clean subset round-trip"
puts "OK f63_bad_hash_validator"
