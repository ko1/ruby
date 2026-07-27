# f61 dedup index: custom #hash/#eql? keys; copied hash looked up with freshly built keys
# axes: copy, custom-hash keys, rehash on receive, GC.start
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

class UserKey
  attr_reader :realm, :uid
  def initialize(realm, uid)
    @realm = realm
    @uid = uid
  end
  def hash
    [realm, uid].hash
  end
  def eql?(other)
    other.is_a?(UserKey) && other.realm == realm && other.uid == uid
  end
  alias == eql?
end

port = Ractor::Port.new
w = Ractor.new(port) do |po|
  mm = Ractor.receive
  hits = (0...4).map { |i| mm[UserKey.new(:web, i)] }
  miss = mm[UserKey.new(:api, 0)]
  po.send([hits, miss, mm.size])
end

idx = {}
4.times { |i| idx[UserKey.new(:web, i)] = "sess-#{i}" }
w.send(idx)
GC.start
hits, miss, sz = port.receive
assert hits == %w[sess-0 sess-1 sess-2 sess-3], "custom-key lookups after copy #{hits.inspect}"
assert miss.nil?, "different realm must miss"
assert sz == 4, "size"
assert idx[UserKey.new(:web, 2)] == "sess-2", "source index intact"
puts "OK f61_custom_key_copy"
