# payload moved A->B->C across three Ractors; compact mid-flight
# axes: move, 3 hops, compact, struct
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
Rc53 = Struct.new(:n, :v)
port = Ractor::Port.new
c = Ractor.new(port) { |o| r = Ractor.receive; o.send([r.class.name, r.v]) }
bx = Ractor.new(c) { |dst| r = Ractor.receive; r.v += 5; dst.send(r, move: true) }
r = Rc53.new("x", 10)
bx.send(r, move: true)
GC.compact
cls, v = port.receive; bx.value; c.value
raise unless cls == "Rc53" && v == 15
puts "OK l53_multihop_move_struct"
