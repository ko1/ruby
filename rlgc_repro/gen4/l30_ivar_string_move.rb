# String carrying generic ivars moved; ivars preserved
# axes: move, generic ivar String, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
w = Ractor.new(port) { |o| s = Ractor.receive; o.send([s.to_s, s.instance_variable_get(:@tag), s.instance_variable_get(:@n)]) }
s = +"payload-30"
s.instance_variable_set(:@tag, "T30")
s.instance_variable_set(:@n, 300)
w.send(s, move: true)
GC.compact
body, tag, n = port.receive; w.value
raise "body #{body}" unless body == "payload-30"
raise "tag #{tag}" unless tag == "T30"
raise "n #{n}" unless n == 300
puts "OK l30_ivar_string_move"
