# Waves of finalizer-registering Ractors terminate and are absorbed
# axes: finalizer registration, respawn waves, absorb
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
4.times do |w|
  port = Ractor::Port.new
  rs = 3.times.map do |k|
    Ractor.new(port, w, k) do |p, wv, kk|
      50.times do |i|
        o = Object.new
        ObjectSpace.define_finalizer(o, proc { })
        GC.start if i % 12 == 0
      end
      p.send(:done)
      wv * 1000 + kk
    end
  end
  3.times { raise unless port.receive == :done }
  raise unless rs.map(&:value).sort == 3.times.map { |k| w * 1000 + k }.sort
  GC.compact if w.odd?
end
puts "OK j35_finalizer_respawn_waves"
