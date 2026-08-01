# f32 packet framer: ASCII-8BIT binary frames built with String#b, moved to parser
# axes: move, BINARY encoding, byte-level framing, husk assert
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

def frame(seq, body)
  hdr = [seq, body.bytesize].pack("CC").b
  (hdr + body.b).b
end

port = Ractor::Port.new
w = Ractor.new(port) do |po|
  loop do
    mm = Ractor.receive
    break if mm == :eof
    seq = mm.getbyte(0)
    len = mm.getbyte(1)
    body = mm.byteslice(2, len)
    po.send([seq, len, body, mm.encoding.name])
  end
end

rounds = STRESS ? 3 : 8
rounds.times do |i|
  body = "payload-#{i}\xFF\xFE".b
  pkt = frame(i, body)
  expect_len = body.bytesize
  w.send(pkt, move: true)
  begin
    pkt.bytesize
    raise "packet not husked"
  rescue Ractor::MovedError
  end
  seq, len, back, enc = port.receive
  assert seq == i, "seq"
  assert len == expect_len, "len #{len}"
  assert back == "payload-#{i}\xFF\xFE".b, "body bytes"
  assert enc == "ASCII-8BIT", "binary encoding, got #{enc}"
end
w.send(:eof)
puts "OK f32_binary_packet_move"
