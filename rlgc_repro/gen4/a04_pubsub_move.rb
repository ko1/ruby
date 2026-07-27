# pub/sub: publisher が move で subscriber に配送、subscriber が集計
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
subs = 4.times.map do |sid|
  Ractor.new(sid) do |id|
    total = 0
    loop do
      msg = Ractor.receive
      break if msg == :stop
      total += msg[:payload].size
    end
    total
  end
end
80.times do |k|
  subs[k % subs.size].send({ topic: :t, payload: Array.new(30) { +"e#{k}-#{_1}" } }, move: true)
  GC.compact if k % 10 == 0
end
subs.each { |s| s.send(:stop) }
raise unless subs.map(&:value).sum == 80 * 30
puts "OK a04"
