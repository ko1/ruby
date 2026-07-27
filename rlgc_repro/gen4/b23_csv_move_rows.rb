# CSV 行 chunk を move で渡し、パース結果 (配列) も move で返す
# axes: 2 workers, move both directions
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 36
port = Ractor::Port.new
ws = 2.times.map do
  Ractor.new(port) do |o|
    loop do
      msg = Ractor.receive
      break if msg == :stop
      ci, chunk = msg
      parsed = chunk.map { |r| r.split(",").map { |x| Integer(x) } }
      o.send([ci, parsed], move: true)
    end
  end
end
idx_chunks = (0...N).each_slice(6).to_a
exp = idx_chunks.map { |c| c.sum { |i| i * 2 + i * 3 } }
nchunks = idx_chunks.size
idx_chunks.each_with_index do |idx, ci|
  rows = idx.map { |i| "#{i * 2},#{i * 3}" }
  ws[ci % 2].send([ci, rows], move: true)
end
got = Array.new(nchunks)
nchunks.times do
  ci, parsed = port.receive
  got[ci] = parsed.sum(&:sum)
end
ws.each { |w| w.send(:stop) }
ws.each(&:value)
raise "got=#{got} exp=#{exp}" unless got == exp
puts "OK b23_csv_move_rows"
