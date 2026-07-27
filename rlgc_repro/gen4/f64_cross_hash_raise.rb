# f64 hostile key: #hash raises only when evaluated OUTSIDE the main ractor; receiver rescues mid-lookup
# axes: copy, custom #hash raising in foreign ractor, legit rescue path
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

MAIN_R = Ractor.current

class HomesickKey
  attr_reader :id
  def initialize(id)
    @id = id
  end
  def hash
    raise RuntimeError, "hashed away from home" unless Ractor.current == MAIN_R
    id.hash
  end
  def eql?(other)
    other.is_a?(HomesickKey) && other.id == id
  end
end

h = { HomesickKey.new(7) => :treasure } # hashing in main: fine

port = Ractor::Port.new
Ractor.new(port) do |po|
  mm = Ractor.receive
  outcome = begin
    [:got, mm[HomesickKey.new(7)]]
  rescue RuntimeError => ex
    [:raised, ex.message]
  end
  # the hash object itself is still usable for non-hashing ops
  po.send([outcome, mm.size, mm.values])
end.send(h)
GC.start
outcome, sz, vals = port.receive
assert outcome == [:raised, "hashed away from home"], "foreign lookup outcome #{outcome.inspect}"
assert sz == 1 && vals == [:treasure], "table body intact"
# main-side lookup still works
assert h[HomesickKey.new(7)] == :treasure, "home lookup fine"
puts "OK f64_cross_hash_raise"
