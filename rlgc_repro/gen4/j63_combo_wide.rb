# Wide fan combining finalizers and weak maps then absorb
# axes: finalizer+weakmap, 8 ractors, absorb
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
ws = 8.times.map do |rid|
  Ractor.new(port, rid) do |p, id|
    wm = ObjectSpace::WeakMap.new
    keep = []
    40.times do |i|
      o = Object.new
      ObjectSpace.define_finalizer(o, proc { })
      wm[i] = o
      keep << o if i % 4 == 0
      GC.start if i % 15 == 0
    end
    GC.start
    live = (0...40).select { |i| i % 4 == 0 }
    ok = live.all? { |i| wm.key?(i) }
    p.send([id, ok])
    live.size
  end
end
8.times { raise unless port.receive.last == true }
exp = (0...40).count { |i| i % 4 == 0 }
raise unless ws.map(&:value).all? { |v| v == exp }
GC.compact
puts "OK j63_combo_wide"
