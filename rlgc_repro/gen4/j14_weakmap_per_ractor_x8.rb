# 8 Ractors each own a WeakMap with compaction on even ids
# axes: weakmap membership, 8 ractors, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
ws = 8.times.map do |rid|
  Ractor.new(port, rid) do |p, id|
    wm = ObjectSpace::WeakMap.new
    kept = []
    50.times { |i| v = "r#{id}_#{i}"; wm[i] = v; kept << v if i % 4 == 0 }
    GC.start
    GC.compact if id.even?
    ok = (0...50).select { |i| i % 4 == 0 }.all? { |i| wm.key?(i) }
    p.send([id, ok])
    kept.size
  end
end
8.times { raise unless port.receive.last == true }
exp = (0...50).count { |i| i % 4 == 0 }
raise unless ws.map(&:value).all? { |v| v == exp }
puts "OK j14_weakmap_per_ractor_x8"
