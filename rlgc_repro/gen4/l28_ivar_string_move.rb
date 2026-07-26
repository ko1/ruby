# String carrying generic ivars moved; ivars preserved
# axes: move, generic ivar String, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
w = Ractor.new(port) { |o| s = Ractor.receive; o.send([s.to_s, s.instance_variable_get(:@tag), s.instance_variable_get(:@n)]) }
s = +"payload-28"
s.instance_variable_set(:@tag, "T28")
s.instance_variable_set(:@n, 280)
w.send(s, move: true)
GC.compact
body, tag, n = port.receive; w.value
raise "body #{body}" unless body == "payload-28"
raise "tag #{tag}" unless tag == "T28"
raise "n #{n}" unless n == 280
puts "OK l28_ivar_string_move"
