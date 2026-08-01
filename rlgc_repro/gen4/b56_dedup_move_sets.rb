# batch を move で dedup worker へ、unique キー配列を move で回収して合併
# axes: 2 workers (キー空間を奇偶で分割), move both ways
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 60
keys = Array.new(N) { |i| (i * 3) % 30 }
exp = keys.uniq.sort

port = Ractor::Port.new
ws = 2.times.map do
  Ractor.new(port) do |o|
    seen = {}
    loop do
      msg = Ractor.receive
      break if msg == :stop
      msg.each { |k| seen[k] = true }
    end
    o.send(seen.keys.sort, move: true)
  end
end
evens = keys.select(&:even?)
odds = keys.select(&:odd?)
ws[0].send(evens, move: true)
ws[1].send(odds, move: true)
ws.each { |w| w.send(:stop) }
merged = []
2.times { merged.concat(port.receive) }
ws.each(&:value)
raise "uniq=#{merged.sort} exp=#{exp}" unless merged.sort == exp
puts "OK b56_dedup_move_sets"
