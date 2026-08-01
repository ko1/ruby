# f51 error catalog: built-in exception instances created (not raised) as payload values, copy
# axes: copy, Exception values, message/class fidelity, GC.start
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

port = Ractor::Port.new
w = Ractor.new(port) do |po|
  mm = Ractor.receive
  po.send(mm.map { |ee| [ee.class.name, ee.message, ee.backtrace.nil?] })
end

catalog = [
  RuntimeError.new("plain runtime"),
  ArgumentError.new("bad arg 42"),
  KeyError.new("missing key"),
  StopIteration.new("done"),
  FrozenError.new("cold"),
]
w.send(catalog)
GC.start
back = port.receive
want = [["RuntimeError", "plain runtime"], ["ArgumentError", "bad arg 42"],
        ["KeyError", "missing key"], ["StopIteration", "done"], ["FrozenError", "cold"]]
back.each_with_index do |(cls, msgv, no_bt), i|
  assert cls == want[i][0], "class #{cls}"
  assert msgv == want[i][1], "message #{msgv}"
  assert no_bt, "unraised exception has nil backtrace"
end
puts "OK f51_exc_catalog_copy"
