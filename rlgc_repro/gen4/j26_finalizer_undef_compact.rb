# define/undefine finalizers plus GC.compact per Ractor
# axes: finalizer register/undefine, 3 ractors, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
ws = 3.times.map do |rid|
  Ractor.new(port, rid) do |p, id|
    made = 0
    120.times do |k|
      o = Object.new
      ObjectSpace.define_finalizer(o, proc { })
      made += 1
      ObjectSpace.undefine_finalizer(o) if k % 5 == 0
      GC.start if k % 16 == 0
    end
    GC.compact
    p.send(made)
    made
  end
end
3.times { raise unless port.receive == 120 }
raise unless ws.map(&:value).all? { |v| v == 120 }
GC.compact
puts "OK j26_finalizer_undef_compact"
