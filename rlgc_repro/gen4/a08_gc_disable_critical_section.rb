# 各 worker が critical section で GC.disable/enable、main は並行に GC 駆動
Warning[:experimental] = false
ws = 6.times.map do |i|
  Ractor.new(i) do |id|
    acc = 0
    50.times do |k|
      GC.disable
      buf = Array.new(30) { +"c#{id}-#{k}-#{_1}" }
      acc += buf.sum(&:bytesize)
      GC.enable
    end
    acc
  end
end
20.times { 3000.times { |k| +"m#{k}" }; GC.start; GC.compact if rand < 0.3 }
raise unless ws.map(&:value).all?(Integer)
raise "gc stuck" unless (c = GC.stat(:count); GC.start; GC.stat(:count) > c)
puts "OK a08"
