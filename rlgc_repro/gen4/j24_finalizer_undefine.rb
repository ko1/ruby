# Mix of define/undefine_finalizer on owned objects across Ractors
# axes: finalizer register/undefine, 4 ractors
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
ws = 4.times.map do |rid|
  Ractor.new(port, rid) do |p, id|
    made = 0
    100.times do |k|
      o = Object.new
      ObjectSpace.define_finalizer(o, proc { })
      made += 1
      ObjectSpace.undefine_finalizer(o) if k % 5 == 0
      GC.start if k % 16 == 0
    end
    p.send(made)
    made
  end
end
4.times { raise unless port.receive == 100 }
raise unless ws.map(&:value).all? { |v| v == 100 }
GC.compact
puts "OK j24_finalizer_undefine"
