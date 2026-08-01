# String carrying generic ivars copied; ivars preserved
# axes: copy, generic ivar String, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
w = Ractor.new(port) { |o| s = Ractor.receive; o.send([s.to_s, s.instance_variable_get(:@tag), s.instance_variable_get(:@n)]) }
s = +"payload-29"
s.instance_variable_set(:@tag, "T29")
s.instance_variable_set(:@n, 290)
w.send(s, move: false)
GC.compact
body, tag, n = port.receive; w.value
raise "body #{body}" unless body == "payload-29"
raise "tag #{tag}" unless tag == "T29"
raise "n #{n}" unless n == 290
puts "OK l29_ivar_string_copy"
