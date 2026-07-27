# wave ごとに 2 段 pipeline (xform->sink) を丸ごと作り直す (respawn 軸)
# axes: 2 waves x 2 chained ractors, copy, wave ごとに検証
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

2.times do |w|
  n = 14
  port = Ractor::Port.new
  sink = Ractor.new(port, n) do |o, cnt|
    tot = 0
    cnt.times { tot += Ractor.receive }
    o.send(tot)
  end
  xf = Ractor.new(sink, w) do |dst, wk|
    loop do
      v = Ractor.receive
      break if v == :stop
      dst.send(v * 3 + wk)
    end
  end
  exp = 0
  n.times do |i|
    v = w * 100 + i
    exp += v * 3 + w
    xf.send(v)
  end
  got = port.receive
  xf.send(:stop)
  xf.value
  sink.value
  raise "wave#{w}: #{got} != #{exp}" unless got == exp
  GC.start
end
puts "OK b77_pipeline_respawn_waves"
