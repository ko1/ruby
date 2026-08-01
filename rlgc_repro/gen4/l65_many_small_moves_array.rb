# many small array objects moved in a stream; periodic compact
# axes: move, streamed small, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
w = Ractor.new(port) do |o|
  total = 0
  while (m = Ractor.receive) != :eof; total += m.sum; end
  o.send(total)
end
exp = 0
120.times do |k|
  a = Array.new(10) { |i| k * 10 + i }
  exp += a.sum
  w.send(a, move: true)
  GC.compact if k % 20 == 0
end
w.send(:eof)
res = port.receive; w.value
raise "small #{res}!=#{exp}" unless res == exp
puts "OK l65_many_small_moves_array"
