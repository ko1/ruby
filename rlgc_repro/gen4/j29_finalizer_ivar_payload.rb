# Objects with ivars and finalizers; retained survivors keep ivar values
# axes: finalizer + generic ivars, 4 ractors, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
ws = 4.times.map do |rid|
  Ractor.new(port, rid) do |p, id|
    keep = []
    100.times do |k|
      o = Object.new
      o.instance_variable_set(:@id, id)
      o.instance_variable_set(:@k, k)
      ObjectSpace.define_finalizer(o, proc { })
      keep << o if k % 10 == 0
      GC.start if k % 20 == 0
    end
    GC.compact
    ok = keep.all? { |x| x.instance_variable_get(:@id) == id }
    p.send(ok)
    keep.size
  end
end
4.times { raise unless port.receive == true }
exp = (100 + 9) / 10
raise unless ws.map(&:value).all? { |v| v == exp }
puts "OK j29_finalizer_ivar_payload"
