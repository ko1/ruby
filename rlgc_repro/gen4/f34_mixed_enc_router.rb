# f34 encoding router: payloads routed to per-encoding workers; encodings preserved through copy
# axes: copy, UTF-8 vs BINARY vs US-ASCII routing, pool, GC.start
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

port = Ractor::Port.new
handlers = {}
%w[UTF-8 ASCII-8BIT US-ASCII].each do |ename|
  handlers[ename] = Ractor.new(port, ename) do |po, myenc|
    loop do
      mm = Ractor.receive
      break if mm == :eof
      po.send([myenc, mm.encoding.name == myenc, mm.bytesize, mm])
    end
  end
end

msgs = [
  "うみ",                        # UTF-8
  "\x01\x02\x03".b,             # ASCII-8BIT
  "plain".encode("US-ASCII"),   # US-ASCII
]
msgs += ["やま", "\xAA\xBB".b] unless STRESS
msgs.each { |m| handlers.fetch(m.encoding.name).send(m) }
GC.start
got = msgs.size.times.map { port.receive }
assert got.all? { |_, okenc, _, _| okenc }, "every payload kept its encoding"
by_enc = got.group_by(&:first)
want_u8 = STRESS ? 1 : 2
want_bin = STRESS ? 1 : 2
assert by_enc["UTF-8"].size == want_u8 && by_enc["ASCII-8BIT"].size == want_bin && by_enc["US-ASCII"].size == 1, "routing counts"
assert by_enc["US-ASCII"][0][3] == "plain", "us-ascii content"
assert by_enc["ASCII-8BIT"].map { |gg| gg[3] }.include?("\x01\x02\x03".b), "binary contents"
handlers.each_value { |h| h.send(:eof) }
puts "OK f34_mixed_enc_router"
