# Wide respawn waves: 6 Ractors per wave each with own WeakMap
# axes: weakmap membership, respawn waves wide
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
exp = (0...30).count { |i| i % 3 == 0 }
3.times do |w|
  port = Ractor::Port.new
  rs = 6.times.map do |k|
    Ractor.new(port, w, k) do |p, wv, kk|
      wm = ObjectSpace::WeakMap.new
      kept = []
      30.times { |i| v = "#{wv}_#{kk}_#{i}"; wm[i] = v; kept << v if i % 3 == 0 }
      GC.start
      ok = (0...30).select { |i| i % 3 == 0 }.all? { |i| wm.key?(i) }
      p.send(ok)
      kept.size
    end
  end
  6.times { raise unless port.receive == true }
  raise unless rs.map(&:value).all? { |v| v == exp }
  GC.compact if w.even?
end
puts "OK j20_weakmap_respawn_wide"
