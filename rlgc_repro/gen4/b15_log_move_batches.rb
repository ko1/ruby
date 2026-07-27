# log batch を move で worker へ渡し、行数と byte 数を返す
# axes: 5 workers, move, batch payload
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

NW = 5
NB = 15
out = Ractor::Port.new
ws = NW.times.map do
  Ractor.new(out) do |o|
    loop do
      msg = Ractor.receive
      break if msg == :stop
      bi, batch = msg
      o.send([bi, batch.size, batch.sum(&:bytesize)])
    end
  end
end
exp = {}
NB.times do |b|
  batch = Array.new(5) { |i| +"w=#{b} line=#{i}" }
  exp[b] = [batch.size, batch.sum(&:bytesize)]
  ws[b % NW].send([b, batch], move: true)
end
got = {}
NB.times do
  bi, n, bytes = out.receive
  got[bi] = [n, bytes]
end
ws.each { |w| w.send(:stop) }
ws.each(&:value)
raise "got=#{got.size}" unless got == exp
puts "OK b15_log_move_batches"
