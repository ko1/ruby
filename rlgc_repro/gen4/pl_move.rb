# gen4 pipeline: 4 stages, every hop MOVES the buffer; each stage appends its
# marker in place. Final sink validates all markers present.
# axes: transfer=move on every hop, GC=none, exceptions=none, payload=medium strings
N_ITEMS = 300

out = Ractor::Port.new

sink = Ractor.new(out) do |o|
  n = len = 0
  while (m = Ractor.receive) != :eos
    raise "markers" unless m.end_with?("|A|B|C")
    n += 1
    len += m.size
  end
  o << [n, len]
end

mk_stage = lambda do |nxt, marker|
  Ractor.new(nxt, marker) do |dst, mk|
    while (m = Ractor.receive) != :eos
      m << mk
      dst.send(m, move: true)
    end
    dst << :eos
  end
end

c = mk_stage.call(sink, "|C")
b = mk_stage.call(c, "|B")
a = mk_stage.call(b, "|A")

base_len = 0
N_ITEMS.times do |i|
  buf = "item-#{i}:" + ("z" * (20 + i % 50))
  base_len += buf.size
  a.send(buf, move: true)
end
a << :eos

n, len = out.receive
[a, b, c, sink].each(&:join)
raise "FAIL n #{n}" unless n == N_ITEMS
raise "FAIL len" unless len == base_len + N_ITEMS * "|A|B|C".size
puts "OK pl_move"
