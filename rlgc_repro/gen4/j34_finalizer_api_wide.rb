# API return-value contract checked across many Ractors
# axes: finalizer API contract, 8 ractors
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
ws = 8.times.map do |rid|
  Ractor.new(port, rid) do |p, id|
    good = 0
    40.times do |k|
      o = Object.new
      ret = ObjectSpace.define_finalizer(o, proc { })
      good += 1 if ret.is_a?(Array)
      good += 1 if ObjectSpace.undefine_finalizer(o).equal?(o)
      GC.start if k % 25 == 0
    end
    p.send(good)
    good
  end
end
8.times { raise unless port.receive == 40 * 2 }
raise unless ws.map(&:value).all? { |v| v == 40 * 2 }
puts "OK j34_finalizer_api_wide"
