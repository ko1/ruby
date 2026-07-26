# 3 Ractors WeakMap survivors with heavy churn
# axes: weakmap membership, 3 ractors, churn
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
ws = 3.times.map do |rid|
  Ractor.new(port, rid) do |p, id|
    wm = ObjectSpace::WeakMap.new
    kept = []
    120.times { |i| v = "r#{id}_#{i}"; wm[i] = v; kept << v if i % 7 == 0 }
    GC.start
    GC.compact if id.even?
    ok = (0...120).select { |i| i % 7 == 0 }.all? { |i| wm.key?(i) }
    p.send([id, ok])
    kept.size
  end
end
3.times { raise unless port.receive.last == true }
exp = (0...120).count { |i| i % 7 == 0 }
raise unless ws.map(&:value).all? { |v| v == exp }
puts "OK j16_weakmap_per_ractor_x3"
