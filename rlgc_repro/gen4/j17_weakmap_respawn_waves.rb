# Waves of Ractors build WeakMaps then terminate; objspace absorb between waves
# axes: weakmap membership, respawn waves, absorb
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
exp = (0...50).count { |i| i % 3 == 0 }
4.times do |w|
  port = Ractor::Port.new
  rs = 3.times.map do |k|
    Ractor.new(port, w, k) do |p, wv, kk|
      wm = ObjectSpace::WeakMap.new
      kept = []
      50.times { |i| v = "#{wv}_#{kk}_#{i}"; wm[i] = v; kept << v if i % 3 == 0 }
      GC.start
      ok = (0...50).select { |i| i % 3 == 0 }.all? { |i| wm.key?(i) }
      p.send(ok)
      kept.size
    end
  end
  3.times { raise unless port.receive == true }
  raise unless rs.map(&:value).all? { |v| v == exp }
  GC.compact if w.even?
end
puts "OK j17_weakmap_respawn_waves"
