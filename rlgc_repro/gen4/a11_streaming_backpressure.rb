# ストリーミング: producer が bounded に送り consumer が処理(receive_if 相当を send/recv で)
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
ack = Ractor::Port.new
consumer = Ractor.new(ack) do |a|
  total = 0
  loop do
    chunk = Ractor.receive
    break if chunk == :eof
    total += chunk.sum(&:bytesize)
    a.send(:ok)
  end
  total
end
100.times do |k|
  consumer.send(Array.new(10) { +"d#{k}-#{_1}" }, move: true)
  ack.receive  # backpressure: 1件ずつ ack
  GC.compact if k % 15 == 0
end
consumer.send(:eof)
raise unless consumer.value.is_a?(Integer)
puts "OK a11"
