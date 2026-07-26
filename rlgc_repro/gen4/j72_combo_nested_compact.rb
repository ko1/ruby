# Nested Ractors with compaction in the innermost Ractor
# axes: finalizer+weakmap, nested ractors, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
outer_port = Ractor::Port.new
outer = Ractor.new(outer_port) do |op|
  inner_port = Ractor::Port.new
  inner = Ractor.new(inner_port) do |ip|
    wm = ObjectSpace::WeakMap.new
    keep = []
    70.times do |i|
      o = Object.new
      ObjectSpace.define_finalizer(o, proc { })
      wm[i] = o
      keep << o if i % 5 == 0
      GC.start if i % 13 == 0
    end
    GC.compact
    ip.send((0...70).select { |i| i % 5 == 0 }.all? { |i| wm.key?(i) })
    keep.size
  end
  iok = inner_port.receive
  ival = inner.value
  op.send([iok, ival])
  ival
end
iok, ival = outer_port.receive
exp = (0...70).count { |i| i % 5 == 0 }
raise unless iok == true && ival == exp
raise unless outer.value == exp
GC.compact
puts "OK j72_combo_nested_compact"
