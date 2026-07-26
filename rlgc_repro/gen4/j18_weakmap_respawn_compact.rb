# Respawn waves with GC.compact on the main Ractor between waves
# axes: weakmap membership, respawn waves, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
exp = (0...40).count { |i| i % 4 == 0 }
3.times do |w|
  port = Ractor::Port.new
  rs = 4.times.map do |k|
    Ractor.new(port, w, k) do |p, wv, kk|
      wm = ObjectSpace::WeakMap.new
      kept = []
      40.times { |i| v = "#{wv}_#{kk}_#{i}"; wm[i] = v; kept << v if i % 4 == 0 }
      GC.start
      ok = (0...40).select { |i| i % 4 == 0 }.all? { |i| wm.key?(i) }
      p.send(ok)
      kept.size
    end
  end
  4.times { raise unless port.receive == true }
  raise unless rs.map(&:value).all? { |v| v == exp }
  GC.compact if w.even?
end
puts "OK j18_weakmap_respawn_compact"
