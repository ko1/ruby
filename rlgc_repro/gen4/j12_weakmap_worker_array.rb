# WeakMap with Array values inside worker; local GC + compact
# axes: weakmap membership, 1 ractor, array values, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
r = Ractor.new(port) do |p|
  wm = ObjectSpace::WeakMap.new
  kept = []
  78.times { |i| v = [i]; wm[i] = v; kept << v if i % 3 == 0 }
  GC.start
  GC.compact
  expect = (0...78).select { |i| i % 3 == 0 }
  expect.each { |i| raise "lost live weak value #{i}" unless wm.key?(i) }
  sz = (0...78).count { |i| wm.key?(i) }
  raise "too many #{sz}" unless sz >= expect.size && sz <= 78
  kept.clear
  p.send(:ok)
  expect.size
end
raise unless port.receive == :ok
raise unless r.value == (0...78).count { |i| i % 3 == 0 }
puts "OK j12_weakmap_worker_array"
