# Wide fan of 8 Ractors each registering finalizers then terminating
# axes: finalizer registration, 8 ractors, absorb
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
ws = 8.times.map do |rid|
  Ractor.new(port, rid) do |p, id|
    made = 0
    60.times do |k|
      o = Object.new
      ObjectSpace.define_finalizer(o, proc { })
      made += 1
      GC.start if k % 16 == 0
    end
    p.send(made)
    made
  end
end
8.times { raise unless port.receive == 60 }
raise unless ws.map(&:value).all? { |v| v == 60 }
GC.compact
puts "OK j27_finalizer_wide"
