# Respawn waves with main-Ractor compaction between waves
# axes: finalizer+weakmap, respawn waves, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
exp = (0...45).count { |i| i % 3 == 0 }
3.times do |w|
  port = Ractor::Port.new
  rs = 4.times.map do |k|
    Ractor.new(port, w, k) do |p, wv, kk|
      wm = ObjectSpace::WeakMap.new
      keep = []
      45.times do |i|
        o = Object.new
        ObjectSpace.define_finalizer(o, proc { })
        wm[i] = o
        keep << o if i % 3 == 0
      end
      GC.start
      ok = (0...45).select { |i| i % 3 == 0 }.all? { |i| wm.key?(i) }
      p.send(ok)
      keep.size
    end
  end
  4.times { raise unless port.receive == true }
  raise unless rs.map(&:value).all? { |v| v == exp }
  GC.compact
end
puts "OK j68_combo_respawn_compact"
