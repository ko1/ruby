# Each Ractor registers finalizers on its own objects while churning
# axes: finalizer registration, 5 ractors, owned objects
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
ws = 5.times.map do |rid|
  Ractor.new(port, rid) do |p, id|
    made = 0
    100.times do |k|
      o = Object.new
      ObjectSpace.define_finalizer(o, proc { })
      made += 1
      GC.start if k % 16 == 0
    end
    p.send(made)
    made
  end
end
5.times { raise unless port.receive == 100 }
raise unless ws.map(&:value).all? { |v| v == 100 }
GC.compact
puts "OK j21_finalizer_owned_churn"
