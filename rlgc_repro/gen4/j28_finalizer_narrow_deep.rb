# Two Ractors registering deep finalizer counts with compaction
# axes: finalizer registration, 2 ractors, deep, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
ws = 2.times.map do |rid|
  Ractor.new(port, rid) do |p, id|
    made = 0
    250.times do |k|
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
2.times { raise unless port.receive == 250 }
raise unless ws.map(&:value).all? { |v| v == 250 }
GC.compact
puts "OK j28_finalizer_narrow_deep"
