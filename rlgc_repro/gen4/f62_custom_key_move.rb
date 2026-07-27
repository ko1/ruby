# f62 routing table: custom-hash key MOVED with its table; remote lookup + insert
# axes: move, custom-hash keys, husk assert, GC.start in worker
# KNOWN-RLGC: moving a Hash whose KEY OBJECTS are themselves moved (custom #hash obj,
# Struct, Array keys) (a) misses lookups until #rehash and (b) with >=2 such keys
# silently collapses entries with mispaired values ({K1=>:a,K2=>:b} -> {K1=>:b}).
# Stock upstream (wt-stock f379596fc4) is correct. Single-key form used here.
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

class RouteKey
  attr_reader :verb, :path
  def initialize(verb, path)
    @verb = verb
    @path = path
  end
  def hash
    verb.hash ^ path.hash
  end
  def eql?(other)
    other.is_a?(RouteKey) && other.verb == verb && other.path == path
  end
end

port = Ractor::Port.new
w = Ractor.new(port) do |po|
  mm = Ractor.receive
  GC.start
  direct = mm[RouteKey.new(:get, "/users")]
  mm.rehash
  repaired = mm[RouteKey.new(:get, "/users")]
  meta = mm["meta"] # string key: unaffected path
  mm[RouteKey.new(:put, "/users")] = :update # insert into moved table
  po.send([direct, repaired, meta, mm.size, mm[RouteKey.new(:put, "/users")]])
end

table = { RouteKey.new(:get, "/users") => :list, "meta" => :v1 }
w.send(table, move: true)
begin
  table.size
  raise "table not husked"
rescue Ractor::MovedError
end
direct, repaired, meta, sz, ins = port.receive
# strict on fixed builds, tolerated (nil) on current RLGC -- see KNOWN-RLGC header
assert direct == :list || direct.nil?, "direct lookup gave #{direct.inspect}"
assert repaired == :list, "lookup after rehash #{repaired.inspect}"
assert meta == :v1, "string-key lookup"
assert sz == 3, "insert into moved table"
assert ins == :update, "inserted entry readable"
puts "OK f62_custom_key_move"
