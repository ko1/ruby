# f36 blob annotator: strings with generic ivars AND non-default encodings, moved
# axes: move, genivar+encoding combo, husk assert, GC.start in worker
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

port = Ractor::Port.new
w = Ractor.new(port, !STRESS) do |po, do_gc|
  loop do
    mm = Ractor.receive
    break if mm == :eof
    GC.start if do_gc # stress mode GCs constantly anyway
    po.send([mm, mm.encoding.name, mm.instance_variable_get(:@mime), mm.instance_variable_get(:@len)])
  end
end

blobs = [
  ["\xDE\xAD\xBE\xEF".b, "application/octet-stream"],
  [+"文字列データ", "text/plain"],
]
blobs.each do |raw, mime|
  s = raw.dup
  # NB: move takes the whole reachable graph -- attach a dup so `mime` stays usable here
  s.instance_variable_set(:@mime, mime.dup)
  s.instance_variable_set(:@len, raw.bytesize)
  want_enc = raw.encoding.name
  w.send(s, move: true)
  begin
    s.bytesize
    raise "blob not husked"
  rescue Ractor::MovedError
  end
  back, enc, gmime, glen = port.receive
  assert back == raw, "bytes for #{mime}"
  assert enc == want_enc, "encoding #{enc} != #{want_enc}"
  assert gmime == mime, "ivar mime"
  assert glen == raw.bytesize, "ivar len"
end
w.send(:eof)
puts "OK f36_string_ivar_enc_move"
