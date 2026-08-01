# WeakMap with Object values inside worker; membership check
# axes: weakmap membership, 1 ractor, object values
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
r = Ractor.new(port) do |p|
  wm = ObjectSpace::WeakMap.new
  kept = []
  88.times { |i| v = Object.new; wm[i] = v; kept << v if i % 3 == 0 }
  GC.start
  expect = (0...88).select { |i| i % 3 == 0 }
  expect.each { |i| raise "lost live weak value #{i}" unless wm.key?(i) }
  sz = (0...88).count { |i| wm.key?(i) }
  raise "too many #{sz}" unless sz >= expect.size && sz <= 88
  kept.clear
  p.send(:ok)
  expect.size
end
raise unless port.receive == :ok
raise unless r.value == (0...88).count { |i| i % 3 == 0 }
puts "OK j09_weakmap_worker_object"
