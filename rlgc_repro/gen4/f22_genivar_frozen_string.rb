# f22 catalog service: ivars set on Strings then frozen; copy keeps frozen bit + ivars
# axes: copy, generic ivars on frozen String, GC.compact
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

port = Ractor::Port.new
w = Ractor.new(port) do |po|
  mm = Ractor.receive
  res = mm.map { |ss| [ss, ss.frozen?, ss.instance_variable_get(:@sku)] }
  # writing to a frozen copy must raise
  frozen_raise = begin
    mm[0] << "x"
    false
  rescue FrozenError
    true
  end
  po.send([res, frozen_raise])
end

items = 4.times.map do |i|
  s = +"item-#{i}"
  s.instance_variable_set(:@sku, "SKU#{1000 + i}")
  s.freeze
end
w.send(items)
GC.compact
res, frozen_raise = port.receive
res.each_with_index do |(txt, fz, sku), i|
  assert txt == "item-#{i}", "text #{i}"
  assert fz, "copy of frozen string must be frozen (#{i})"
  assert sku == "SKU#{1000 + i}", "ivar on frozen string #{i}"
end
assert frozen_raise, "mutation of frozen copy must raise FrozenError"
puts "OK f22_genivar_frozen_string"
