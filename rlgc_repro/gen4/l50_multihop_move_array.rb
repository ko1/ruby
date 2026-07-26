# payload moved A->B->C across three Ractors; compact mid-flight
# axes: move, 3 hops, compact, array
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
c = Ractor.new(port) { |o| a = Ractor.receive; o.send(a.sum) }
bx = Ractor.new(c) { |dst| a = Ractor.receive; a.map! { |x| x + 1 }; dst.send(a, move: true) }
a = Array.new(300) { |i| i }
exp = (0...300).map { |i| i + 1 }.sum
bx.send(a, move: true)
GC.compact
res = port.receive; bx.value; c.value
raise "hop #{res}!=#{exp}" unless res == exp
puts "OK l50_multihop_move_array"
