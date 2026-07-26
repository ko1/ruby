# Three Ractors, combined stress, compaction each
# axes: finalizer+weakmap, 3 ractors, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
ws = 3.times.map do |rid|
  Ractor.new(port, rid) do |p, id|
    wm = ObjectSpace::WeakMap.new
    keep = []
    90.times do |i|
      o = Object.new
      ObjectSpace.define_finalizer(o, proc { })
      wm[i] = o
      keep << o if i % 4 == 0
      GC.start if i % 15 == 0
    end
    GC.compact
    live = (0...90).select { |i| i % 4 == 0 }
    ok = live.all? { |i| wm.key?(i) }
    p.send([id, ok])
    live.size
  end
end
3.times { raise unless port.receive.last == true }
exp = (0...90).count { |i| i % 4 == 0 }
raise unless ws.map(&:value).all? { |v| v == exp }
GC.compact
puts "OK j66_combo_x3_compact"
