# Large finalizer counts per Ractor with periodic GC
# axes: finalizer registration, 3 ractors, many
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
ws = 3.times.map do |rid|
  Ractor.new(port, rid) do |p, id|
    made = 0
    200.times do |k|
      o = Object.new
      ObjectSpace.define_finalizer(o, proc { })
      made += 1
      GC.start if k % 16 == 0
    end
    p.send(made)
    made
  end
end
3.times { raise unless port.receive == 200 }
raise unless ws.map(&:value).all? { |v| v == 200 }
GC.compact
puts "OK j25_finalizer_many"
