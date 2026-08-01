# large copy snapshot under compaction; sender retains object
# axes: copy, snapshot, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port1 = Ractor::Port.new; port2 = Ractor::Port.new
w1 = Ractor.new(port1) { |o| a = Ractor.receive; o.send(a.sum) }
w2 = Ractor.new(port2) { |o| a = Ractor.receive; o.send(a.size) }
a = Array.new(800) { |i| i }
w1.send(a); w2.send(a)  # copy to both, sender retains
GC.compact
s = port1.receive; sz = port2.receive; w1.value; w2.value
raise unless s == (0...800).sum && sz == 800 && a.sum == (0...800).sum
puts "OK l61_copy_snapshot_twice"
