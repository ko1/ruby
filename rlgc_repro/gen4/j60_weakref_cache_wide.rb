# Wide fan of per-Ractor weak caches with compaction
# axes: weak cache, 8 ractors, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
ws = 8.times.map do |rid|
  Ractor.new(port, rid) do |p, id|
    cache = ObjectSpace::WeakMap.new
    pins = {}
    120.times do |i|
      val = "v#{id}_#{i}"
      cache[i] = val
      pins[i] = val if i % 3 == 0
    end
    GC.start
    GC.compact if id.even?
    ok = pins.keys.all? { |k| cache.key?(k) }
    p.send(ok)
    pins.size
  end
end
8.times { raise unless port.receive == true }
exp = (0...120).count { |i| i % 3 == 0 }
raise unless ws.map(&:value).all? { |v| v == exp }
puts "OK j60_weakref_cache_wide"
