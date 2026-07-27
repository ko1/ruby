# c54: scatter-gather in successive waves with per-wave worker respawn;
# main stress only in wave 0, participant stress afterwards; compact between waves.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false

WAVES = STRESS ? 2 : 4
W = STRESS ? 3 : 4
PER = STRESS ? 6 : 25

grand = 0
expected = 0
WAVES.times do |wv|
  GC.stress = true if STRESS && wv == 0
  data = (0...(W * PER)).map { |i| (i * (wv + 3)) % 41 }
  expected += data.sum
  gather = Ractor::Port.new
  ws = W.times.map do |i|
    Ractor.new(gather, i, data[i * PER, PER], wv) do |g, wid, chunk, wave|
      GC.stress = true if ENV['S_STRESS'] && wave > 0
      s = chunk.sum
      GC.stress = false
      g << [:partial, wid, s]
      :wave_done
    end
  end
  W.times do
    tag, _wid, s = gather.receive
    raise "partial" unless tag == :partial
    grand += s
  end
  GC.stress = false
  ws.each { |r| raise unless r.value == :wave_done }
  GC.compact if wv == 1 && !STRESS
end
raise "grand" unless grand == expected
puts "OK c54_scatter_waves"
