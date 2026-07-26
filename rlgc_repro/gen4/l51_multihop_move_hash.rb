# payload moved A->B->C across three Ractors; compact mid-flight
# axes: move, 3 hops, compact, hash
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
c = Ractor.new(port) { |o| h = Ractor.receive; o.send(h.values.sum) }
bx = Ractor.new(c) { |dst| h = Ractor.receive; h.transform_values! { |v| v * 2 }; dst.send(h, move: true) }
h = {}; 200.times { |i| h[i] = i }
exp = (0...200).map { |i| i * 2 }.sum
bx.send(h, move: true)
GC.compact
res = port.receive; bx.value; c.value
raise "hop #{res}!=#{exp}" unless res == exp
puts "OK l51_multihop_move_hash"
