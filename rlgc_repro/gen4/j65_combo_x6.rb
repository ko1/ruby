# Six Ractors combining finalizers with weak-map survivors
# axes: finalizer+weakmap, 6 ractors
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
ws = 6.times.map do |rid|
  Ractor.new(port, rid) do |p, id|
    wm = ObjectSpace::WeakMap.new
    keep = []
    55.times do |i|
      o = Object.new
      ObjectSpace.define_finalizer(o, proc { })
      wm[i] = o
      keep << o if i % 4 == 0
      GC.start if i % 15 == 0
    end
    GC.start
    live = (0...55).select { |i| i % 4 == 0 }
    ok = live.all? { |i| wm.key?(i) }
    p.send([id, ok])
    live.size
  end
end
6.times { raise unless port.receive.last == true }
exp = (0...55).count { |i| i % 4 == 0 }
raise unless ws.map(&:value).all? { |v| v == exp }
GC.compact
puts "OK j65_combo_x6"
