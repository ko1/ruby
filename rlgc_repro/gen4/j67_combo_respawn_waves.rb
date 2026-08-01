# Respawn waves of combined finalizer+weakmap Ractors; absorb between waves
# axes: finalizer+weakmap, respawn waves
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
exp = (0...50).count { |i| i % 3 == 0 }
4.times do |w|
  port = Ractor::Port.new
  rs = 3.times.map do |k|
    Ractor.new(port, w, k) do |p, wv, kk|
      wm = ObjectSpace::WeakMap.new
      keep = []
      50.times do |i|
        o = Object.new
        ObjectSpace.define_finalizer(o, proc { })
        wm[i] = o
        keep << o if i % 3 == 0
      end
      GC.start
      ok = (0...50).select { |i| i % 3 == 0 }.all? { |i| wm.key?(i) }
      p.send(ok)
      keep.size
    end
  end
  3.times { raise unless port.receive == true }
  raise unless rs.map(&:value).all? { |v| v == exp }
  GC.start
end
puts "OK j67_combo_respawn_waves"
