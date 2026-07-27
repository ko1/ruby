# f20 cache service: Data instances as Hash keys (value-based #hash/#eql?), copy round-trip lookups
# axes: copy, Data keys, hash rehash on receive, GC.start
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

CKey = Data.define(:ns, :id)

port = Ractor::Port.new
w = Ractor.new(port) do |po|
  loop do
    mm = Ractor.receive
    break if mm == :eof
    # look up with freshly built keys: relies on value #hash surviving the copy
    hits = (0...4).map { |i| mm[CKey.new(ns: :user, id: i)] }
    miss = mm[CKey.new(ns: :user, id: 99)]
    po.send([hits, miss, mm.size])
  end
end

cache = {}
4.times { |i| cache[CKey.new(ns: :user, id: i)] = "u#{i}" }
w.send(cache)
GC.start
hits, miss, sz = port.receive
assert hits == %w[u0 u1 u2 u3], "lookup after copy #{hits.inspect}"
assert miss.nil?, "missing key must miss"
assert sz == 4, "size"
# local lookups still fine
assert cache[CKey.new(ns: :user, id: 2)] == "u2", "source cache intact"
w.send(:eof)
puts "OK f20_data_cache_keys"
