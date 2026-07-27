# 4段パイプライン: source -> upcase -> tag -> sink、各段が独立 Ractor
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
sink = Ractor::Port.new
s4 = Ractor.new(sink) { |snk| loop { m = Ractor.receive; break(snk.send(:stop)) if m == :stop; snk.send(m) } }
s3 = Ractor.new(s4) { |out| loop { m = Ractor.receive; break(out.send(:stop)) if m == :stop; GC.compact if rand < 0.05; out.send(m.merge(tagged: true)) } }
s2 = Ractor.new(s3) { |out| loop { m = Ractor.receive; break(out.send(:stop)) if m == :stop; out.send(m.merge(up: m[:s].upcase)) } }
100.times { |i| s2.send({ i: i, s: "row#{i}" }) }
s2.send(:stop)
out = []
loop { m = sink.receive; break if m == :stop; out << m }
[s2, s3, s4].each(&:value)
raise "bad #{out.size}" unless out.size == 100 && out.all? { |h| h[:tagged] && h[:up] == h[:s].upcase }
puts "OK a02"
