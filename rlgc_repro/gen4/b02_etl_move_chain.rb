# ETL chain を move 転送で: batch を各段が move で次段へ渡す (source は husk)
# axes: 3 ractors, move, batches of strings
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 24
sink = Ractor::Port.new
loader = Ractor.new(sink) do |out|
  sum = 0
  n = 0
  while (batch = Ractor.receive) != :eof
    sum += batch.sum(&:bytesize)
    n += 1
  end
  out.send([n, sum])
end
xf = Ractor.new(loader) do |dst|
  while (batch = Ractor.receive) != :eof
    batch.map! { |s| s.upcase << "!" }
    dst.send(batch, move: true)
  end
  dst.send(:eof)
end
expected = 0
N.times do |k|
  batch = Array.new(4) { |i| +"item-#{k}-#{i}" }
  expected += batch.sum { |s| s.bytesize + 1 } # upcase keeps size, << "!" adds 1
  xf.send(batch, move: true)
end
xf.send(:eof)
n, sum = sink.receive
xf.value
loader.value
raise "n=#{n}" unless n == N
raise "sum=#{sum} exp=#{expected}" unless sum == expected
puts "OK b02_etl_move_chain"
