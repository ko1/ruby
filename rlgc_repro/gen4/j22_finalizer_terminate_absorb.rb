# Workers register finalizers then terminate; objspace absorb by main
# axes: finalizer registration, 6 ractors, terminate absorb
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
ws = 6.times.map do |rid|
  Ractor.new(port, rid) do |p, id|
    made = 0
    80.times do |k|
      o = Object.new
      ObjectSpace.define_finalizer(o, proc { })
      made += 1
      GC.start if k % 16 == 0
    end
    p.send(made)
    made
  end
end
6.times { raise unless port.receive == 80 }
raise unless ws.map(&:value).all? { |v| v == 80 }
GC.compact
puts "OK j22_finalizer_terminate_absorb"
