# f21 tagging service: generic ivars on plain Strings, copy round-trip preserves tags
# axes: copy, generic ivars on String, GC.start
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

port = Ractor::Port.new
w = Ractor.new(port) do |po|
  loop do
    mm = Ractor.receive
    break if mm == :eof
    po.send(mm.map { |ss| [ss, ss.instance_variable_get(:@tag), ss.instance_variable_get(:@score)] })
  end
end

docs = STRESS ? 3 : 10
batch = docs.times.map do |i|
  s = +"document-#{i}"
  s.instance_variable_set(:@tag, :"t#{i % 3}")
  s.instance_variable_set(:@score, i * 1.5)
  s
end
w.send(batch)
GC.start
back = port.receive
back.each_with_index do |(txt, tag, score), i|
  assert txt == "document-#{i}", "text #{i}"
  assert tag == :"t#{i % 3}", "generic ivar tag #{i}"
  assert score == i * 1.5, "generic ivar score #{i}"
end
# source strings keep their generic ivars
assert batch[0].instance_variable_get(:@tag) == :t0, "source ivar intact"
w.send(:eof)
puts "OK f21_genivar_string_tagger"
