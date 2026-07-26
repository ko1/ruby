# 4 Ractors each own a WeakMap; live values survive per-Ractor local GC
# axes: weakmap membership, 4 ractors, per-ractor local GC
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
ws = 4.times.map do |rid|
  Ractor.new(port, rid) do |p, id|
    wm = ObjectSpace::WeakMap.new
    kept = []
    60.times { |i| v = "r#{id}_#{i}"; wm[i] = v; kept << v if i % 3 == 0 }
    GC.start
    GC.compact if id.even?
    ok = (0...60).select { |i| i % 3 == 0 }.all? { |i| wm.key?(i) }
    p.send([id, ok])
    kept.size
  end
end
4.times { raise unless port.receive.last == true }
exp = (0...60).count { |i| i % 3 == 0 }
raise unless ws.map(&:value).all? { |v| v == exp }
puts "OK j13_weakmap_per_ractor_x4"
