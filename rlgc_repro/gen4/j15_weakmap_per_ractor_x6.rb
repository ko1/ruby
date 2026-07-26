# 6 Ractors each build/probe a WeakMap then terminate (absorb)
# axes: weakmap membership, 6 ractors, terminate absorb
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
ws = 6.times.map do |rid|
  Ractor.new(port, rid) do |p, id|
    wm = ObjectSpace::WeakMap.new
    kept = []
    70.times { |i| v = "r#{id}_#{i}"; wm[i] = v; kept << v if i % 5 == 0 }
    GC.start
    GC.compact if id.even?
    ok = (0...70).select { |i| i % 5 == 0 }.all? { |i| wm.key?(i) }
    p.send([id, ok])
    kept.size
  end
end
6.times { raise unless port.receive.last == true }
exp = (0...70).count { |i| i % 5 == 0 }
raise unless ws.map(&:value).all? { |v| v == exp }
puts "OK j15_weakmap_per_ractor_x6"
