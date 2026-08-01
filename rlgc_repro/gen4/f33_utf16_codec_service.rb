# f33 codec service: UTF-16LE where loadable (graceful fallback to byte path when enc ext absent)
# axes: copy, cross-encoding round-trip, GC.start
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

probe = "x".encode("UTF-16LE") rescue nil
U16 = !probe.nil? && probe.encoding.name == "UTF-16LE"

port = Ractor::Port.new
w = Ractor.new(port, U16) do |po, u16ok|
  loop do
    mm = Ractor.receive
    break if mm == :eof
    if u16ok
      wide = mm.encode("UTF-16LE")
      po.send([wide.encoding.name, wide.bytesize, wide.encode("UTF-8")])
    else
      bb = mm.b
      po.send([bb.encoding.name, bb.bytesize, bb.force_encoding("UTF-8")])
    end
  end
end

texts = ["abc", "héllo", "テスト"]
texts.each do |t|
  w.send(t)
  ename, bs, round = port.receive
  if U16
    assert ename == "UTF-16LE", "wide encoding #{ename}"
    assert bs == t.encode("UTF-16LE").bytesize && bs >= 2 * t.length, "utf16 bytesize"
  else
    assert ename == "ASCII-8BIT", "fallback encoding #{ename}"
    assert bs == t.bytesize, "byte path size"
  end
  assert round == t, "round-trip for #{t}"
end
GC.start
w.send(:eof)
puts "OK f33_utf16_codec_service"
