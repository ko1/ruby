# f79 chunk streamer: worker streams MOVED chunks through the result port; main reassembles
# axes: Port#send(move:), streaming lifecycle, GC.start during stream
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

port = Ractor::Port.new
w = Ractor.new(port) do |po|
  nchunks, size = Ractor.receive
  nchunks.times do |i|
    chunk = { seq: i, bytes: ("%02d" % (i % 100)) * (size / 2) }
    po.send(chunk, move: true)
    begin
      chunk[:seq]
      raise "sender kept moved chunk"
    rescue Ractor::MovedError
      nil
    end
  end
  po.send(:fin)
end

nchunks = STRESS ? 4 : 12
w.send([nchunks, 64])
buf = []
loop do
  mm = port.receive
  break if mm == :fin
  buf << mm
  GC.start if buf.size == 2
end
assert buf.size == nchunks, "chunk count #{buf.size}"
buf.each_with_index do |cc, i|
  assert cc[:seq] == i, "order"
  assert cc[:bytes] == ("%02d" % (i % 100)) * 32, "chunk #{i} content"
  cc[:bytes] << "+" # moved-in chunks are owned and mutable here
end
assert buf.all? { |cc| cc[:bytes].end_with?("+") }, "ownership"
puts "OK f79_port_move_stream"
