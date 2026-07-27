# event batch を move で流し、worker が tumbling window 集計を move で返す
# axes: 2 workers, move, batch = 1 window
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

NB = 12
exp = Array.new(NB) { |b| Array.new(6) { |i| b * 10 + i }.sum }
port = Ractor::Port.new
ws = 2.times.map do
  Ractor.new(port) do |o|
    loop do
      msg = Ractor.receive
      break if msg == :stop
      bi, evs = msg
      o.send([bi, evs.sum { |e| e[:v] }], move: true)
    end
  end
end
NB.times do |b|
  evs = Array.new(6) { |i| { t: b * 6 + i, v: b * 10 + i } }
  ws[b % 2].send([b, evs], move: true)
end
got = Array.new(NB)
NB.times do
  bi, s = port.receive
  got[bi] = s
end
ws.each { |w| w.send(:stop) }
ws.each(&:value)
raise "got=#{got} exp=#{exp}" unless got == exp
puts "OK b50_window_move_events"
