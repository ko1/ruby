# Mix define/undefine finalizer with weak-map survivors per Ractor
# axes: finalizer undefine + weakmap, 5 ractors
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
ws = 5.times.map do |rid|
  Ractor.new(port, rid) do |p, id|
    wm = ObjectSpace::WeakMap.new
    keep = []
    60.times do |i|
      o = Object.new
      ObjectSpace.define_finalizer(o, proc { })
      ObjectSpace.undefine_finalizer(o) if i.even?
      wm[i] = o
      keep << o if i % 6 == 0
    end
    GC.start
    GC.compact if id.odd?
    ok = (0...60).select { |i| i % 6 == 0 }.all? { |i| wm.key?(i) }
    p.send(ok)
    keep.size
  end
end
5.times { raise unless port.receive == true }
exp = (0...60).count { |i| i % 6 == 0 }
raise unless ws.map(&:value).all? { |v| v == exp }
puts "OK j76_combo_undefine_weakmap"
