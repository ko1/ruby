# f10 mirror service: cyclic + internally-shared structure COPY preserves topology
# axes: copy with cycles, shared-substructure identity, GC.start mid
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

port = Ractor::Port.new
w = Ractor.new(port) do |po|
  mm = Ractor.receive
  # copy must preserve: cycle, and the SAME shared sub-array referenced twice
  po.send([mm[:self].equal?(mm), mm[:a].equal?(mm[:b]), mm[:a] == [7, 8, 9]])
end

shared = [7, 8, 9]
doc = { a: shared, b: shared }
doc[:self] = doc
w.send(doc)
GC.start
cyc, shr, dat = port.receive
assert cyc, "copy dropped self-cycle"
assert shr, "copy duplicated shared substructure"
assert dat, "copy corrupted data"
# source stays fully usable after copy
assert doc[:self].equal?(doc) && doc[:a].equal?(shared), "source damaged by copy"
puts "OK f10_cycle_copy_service"
