# Wide combined respawn: 6 Ractors per wave
# axes: finalizer+weakmap, respawn wide
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
exp = (0...35).count { |i| i % 3 == 0 }
3.times do |w|
  port = Ractor::Port.new
  rs = 6.times.map do |k|
    Ractor.new(port, w, k) do |p, wv, kk|
      wm = ObjectSpace::WeakMap.new
      keep = []
      35.times do |i|
        o = Object.new
        ObjectSpace.define_finalizer(o, proc { })
        wm[i] = o
        keep << o if i % 3 == 0
      end
      GC.start
      ok = (0...35).select { |i| i % 3 == 0 }.all? { |i| wm.key?(i) }
      p.send(ok)
      keep.size
    end
  end
  6.times { raise unless port.receive == true }
  raise unless rs.map(&:value).all? { |v| v == exp }
  GC.compact
end
puts "OK j70_combo_respawn_wide"
