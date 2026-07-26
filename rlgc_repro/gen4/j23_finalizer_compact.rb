# Finalizer registration followed by GC.compact inside each Ractor
# axes: finalizer registration, 4 ractors, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
ws = 4.times.map do |rid|
  Ractor.new(port, rid) do |p, id|
    made = 0
    90.times do |k|
      o = Object.new
      ObjectSpace.define_finalizer(o, proc { })
      made += 1
      GC.start if k % 16 == 0
    end
    GC.compact
    p.send(made)
    made
  end
end
4.times { raise unless port.receive == 90 }
raise unless ws.map(&:value).all? { |v| v == 90 }
GC.compact
puts "OK j23_finalizer_compact"
