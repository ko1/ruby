# payload moved A->B->C across three Ractors; compact mid-flight
# axes: move, 3 hops, compact, mixed
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
c = Ractor.new(port) { |o| g = Ractor.receive; o.send(g[:nums].sum + g[:extra]) }
bx = Ractor.new(c) { |dst| g = Ractor.receive; g[:extra] = 100; dst.send(g, move: true) }
g = { nums: (1..100).to_a, extra: 0, label: "L" }
exp = (1..100).sum + 100
bx.send(g, move: true)
GC.compact
res = port.receive; bx.value; c.value
raise "hop #{res}!=#{exp}" unless res == exp
puts "OK l54_multihop_move_mixed"
