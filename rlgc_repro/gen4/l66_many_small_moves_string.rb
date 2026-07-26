# many small string objects moved in a stream; periodic compact
# axes: move, streamed small, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
w = Ractor.new(port) do |o|
  total = 0
  while (m = Ractor.receive) != :eof; total += m.bytesize; end
  o.send(total)
end
exp = 0
150.times do |k|
  s = +"chunk-#{k}-payload"
  exp += s.bytesize
  w.send(s, move: true)
  GC.compact if k % 30 == 0
end
w.send(:eof)
res = port.receive; w.value
raise "small #{res}!=#{exp}" unless res == exp
puts "OK l66_many_small_moves_string"
