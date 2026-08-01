# Wide fan of define/undefine + weakmap Ractors with compaction
# axes: finalizer undefine + weakmap, 8 ractors, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
ws = 8.times.map do |rid|
  Ractor.new(port, rid) do |p, id|
    wm = ObjectSpace::WeakMap.new
    keep = []
    42.times do |i|
      o = Object.new
      ObjectSpace.define_finalizer(o, proc { })
      ObjectSpace.undefine_finalizer(o) if i.even?
      wm[i] = o
      keep << o if i % 6 == 0
    end
    GC.start
    GC.compact if id.odd?
    ok = (0...42).select { |i| i % 6 == 0 }.all? { |i| wm.key?(i) }
    p.send(ok)
    keep.size
  end
end
8.times { raise unless port.receive == true }
exp = (0...42).count { |i| i % 6 == 0 }
raise unless ws.map(&:value).all? { |v| v == exp }
puts "OK j77_combo_undefine_wide"
