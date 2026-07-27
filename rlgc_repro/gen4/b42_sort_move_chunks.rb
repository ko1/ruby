# chunk を move で sorter へ渡し、sort 済み chunk を move で回収して連結検証
# axes: 3 workers, move both ways, indexed chunks
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 48
NW = 3
vals = Array.new(N) { |i| (i * 137 + 5) % 211 }
exp = vals.sort
port = Ractor::Port.new
ws = NW.times.map do
  Ractor.new(port) do |o|
    loop do
      msg = Ractor.receive
      break if msg == :stop
      ci, chunk = msg
      chunk.sort!
      o.send([ci, chunk], move: true)
    end
  end
end
cs = N / NW
NW.times do |c|
  chunk = vals[c * cs, cs]
  ws[c].send([c, chunk], move: true)
end
chunks = Array.new(NW)
NW.times do
  ci, chunk = port.receive
  chunks[ci] = chunk
end
ws.each { |w| w.send(:stop) }
ws.each(&:value)
merged = chunks.flatten.sort # chunk ごとに sort 済みかも確認
chunks.each { |c| raise "chunk unsorted" unless c == c.sort }
raise "merged" unless merged == exp
puts "OK b42_sort_move_chunks"
