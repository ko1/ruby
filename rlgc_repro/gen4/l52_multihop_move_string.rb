# payload moved A->B->C across three Ractors; compact mid-flight
# axes: move, 3 hops, compact, string
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
c = Ractor.new(port) { |o| s = Ractor.receive; o.send(s.length) }
bx = Ractor.new(c) { |dst| s = Ractor.receive; s << "XYZ"; dst.send(s, move: true) }
s = +("m" * 3000)
bx.send(s, move: true)
GC.compact
res = port.receive; bx.value; c.value
raise "hop #{res}" unless res == 3003
puts "OK l52_multihop_move_string"
