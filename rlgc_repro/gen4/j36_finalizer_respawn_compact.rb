# Respawn waves of finalizer Ractors with compaction between waves
# axes: finalizer registration, respawn waves, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
3.times do |w|
  port = Ractor::Port.new
  rs = 4.times.map do |k|
    Ractor.new(port, w, k) do |p, wv, kk|
      40.times do |i|
        o = Object.new
        ObjectSpace.define_finalizer(o, proc { })
        GC.start if i % 12 == 0
      end
      p.send(:done)
      wv * 1000 + kk
    end
  end
  4.times { raise unless port.receive == :done }
  raise unless rs.map(&:value).sort == 4.times.map { |k| w * 1000 + k }.sort
  GC.compact if w.odd?
end
puts "OK j36_finalizer_respawn_compact"
