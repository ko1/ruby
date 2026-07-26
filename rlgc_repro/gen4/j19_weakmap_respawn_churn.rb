# High-churn WeakMap respawn waves across many short-lived Ractors
# axes: weakmap membership, respawn waves, churn
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
exp = (0...80).count { |i| i % 6 == 0 }
5.times do |w|
  port = Ractor::Port.new
  rs = 2.times.map do |k|
    Ractor.new(port, w, k) do |p, wv, kk|
      wm = ObjectSpace::WeakMap.new
      kept = []
      80.times { |i| v = "#{wv}_#{kk}_#{i}"; wm[i] = v; kept << v if i % 6 == 0 }
      GC.start
      ok = (0...80).select { |i| i % 6 == 0 }.all? { |i| wm.key?(i) }
      p.send(ok)
      kept.size
    end
  end
  2.times { raise unless port.receive == true }
  raise unless rs.map(&:value).all? { |v| v == exp }
  GC.compact if w.even?
end
puts "OK j19_weakmap_respawn_churn"
