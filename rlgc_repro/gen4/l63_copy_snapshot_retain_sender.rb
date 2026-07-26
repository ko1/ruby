# large copy snapshot under compaction; sender retains object
# axes: copy, snapshot, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
w = Ractor.new(port) { |o| h = Ractor.receive; o.send(h.size) }
h = {}; 400.times { |i| h["k#{i}"] = Array.new(3) { |j| i + j } }
w.send(h)  # copy, main keeps h
GC.compact
sz = port.receive; w.value
raise unless sz == 400
raise unless h["k0"] == [0, 1, 2]
raise unless h.size == 400
puts "OK l63_copy_snapshot_retain_sender"
