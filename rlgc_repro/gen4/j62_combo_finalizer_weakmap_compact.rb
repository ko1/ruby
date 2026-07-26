# Combined finalizer+weakmap workers with per-Ractor compaction
# axes: finalizer+weakmap, 5 ractors, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
ws = 5.times.map do |rid|
  Ractor.new(port, rid) do |p, id|
    wm = ObjectSpace::WeakMap.new
    keep = []
    50.times do |i|
      o = Object.new
      ObjectSpace.define_finalizer(o, proc { })
      wm[i] = o
      keep << o if i % 4 == 0
      GC.start if i % 15 == 0
    end
    GC.compact
    live = (0...50).select { |i| i % 4 == 0 }
    ok = live.all? { |i| wm.key?(i) }
    p.send([id, ok])
    live.size
  end
end
5.times { raise unless port.receive.last == true }
exp = (0...50).count { |i| i % 4 == 0 }
raise unless ws.map(&:value).all? { |v| v == exp }
GC.compact
puts "OK j62_combo_finalizer_weakmap_compact"
