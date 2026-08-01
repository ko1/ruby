# many small hash objects moved in a stream; periodic compact
# axes: move, streamed small, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
w = Ractor.new(port) do |o|
  total = 0
  while (m = Ractor.receive) != :eof; total += m.values.sum; end
  o.send(total)
end
exp = 0
120.times do |k|
  h = { a: k, b: k * 2, c: k * 3 }
  exp += h.values.sum
  w.send(h, move: true)
  GC.compact if k % 25 == 0
end
w.send(:eof)
res = port.receive; w.value
raise "small #{res}!=#{exp}" unless res == exp
puts "OK l67_many_small_moves_hash"
