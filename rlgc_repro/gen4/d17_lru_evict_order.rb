# Session cache with LRU eviction; service eviction sequence == main-side model replay.
# Axes: cap=8, 120 seeded ops, copy, stress in service.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
class LRU
  attr_reader :evicted
  def initialize(cap) = (@cap = cap; @h = {}; @evicted = [])
  def put(k, v)
    @h.delete(k); @h[k] = v
    if @h.size > @cap
      ek, ev = @h.first
      @h.delete(ek); @evicted << [ek, ev]
    end
  end
  def get(k)
    return nil unless @h.key?(k)
    v = @h.delete(k); @h[k] = v; v
  end
  def size = @h.size
end
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  c = LRU.new(8)
  loop do
    msg = Ractor.receive
    break if msg == :stop
    op, k, v, rp = msg
    case op
    when :put then c.put(k, v); rp << :ok
    when :get then rp << c.get(k)
    end
  end
  GC.stress = false
  done << :done
  [c.evicted, c.size]
end
rp = Ractor::Port.new
model = LRU.new(8)
rng = Random.new(17)
120.times do |i|
  k = "s#{rng.rand(14)}"
  if rng.rand(2) == 0
    v = "sess-#{i}"
    model.put(k, v)
    svc.send([:put, k, v, rp])
    raise unless rp.receive == :ok
  else
    svc.send([:get, k, nil, rp])
    raise "get#{i}" unless rp.receive == model.get(k)
  end
end
svc.send(:stop)
done.receive
ev, sz = svc.value
raise "evict order" unless ev == model.evicted
raise "size" unless sz == model.size
puts "OK d17_lru_evict_order"
