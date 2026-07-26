# WeakMap built and probed inside a worker Ractor; local GC survivors
# axes: weakmap membership, 1 ractor, per-ractor local GC
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
r = Ractor.new(port) do |p|
  wm = ObjectSpace::WeakMap.new
  kept = []
  80.times { |i| v = "w#{i}"; wm[i] = v; kept << v if i % 2 == 0 }
  GC.start
  expect = (0...80).select { |i| i % 2 == 0 }
  expect.each { |i| raise "lost live weak value #{i}" unless wm.key?(i) }
  sz = (0...80).count { |i| wm.key?(i) }
  raise "too many #{sz}" unless sz >= expect.size && sz <= 80
  kept.clear
  p.send(:ok)
  expect.size
end
raise unless port.receive == :ok
raise unless r.value == (0...80).count { |i| i % 2 == 0 }
puts "OK j04_weakmap_in_worker"
