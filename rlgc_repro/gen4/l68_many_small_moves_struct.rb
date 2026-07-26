# many small struct objects moved in a stream; periodic compact
# axes: move, streamed small, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
Sm68 = Struct.new(:v)
port = Ractor::Port.new
w = Ractor.new(port) do |o|
  total = 0
  while (m = Ractor.receive) != :eof; total += m.v; end
  o.send(total)
end
exp = 0
120.times do |k|
  r = Sm68.new(k)
  exp += k
  w.send(r, move: true)
  GC.compact if k % 25 == 0
end
w.send(:eof)
res = port.receive; w.value
raise "small #{res}!=#{exp}" unless res == exp
puts "OK l68_many_small_moves_struct"
