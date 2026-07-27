# f55 error digest: exceptions embedded deep inside Hash/Array payloads (values, not raised)
# axes: copy, Exception leaves at depth 5, GC.compact
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

def report(depth, i)
  return { err: RuntimeError.new("leaf-#{i}"), at: i } if depth == 0
  { level: depth, entries: [report(depth - 1, i * 2), report(depth - 1, i * 2 + 1)] }
end

port = Ractor::Port.new
w = Ractor.new(port) do |po|
  mm = Ractor.receive
  GC.compact
  msgs = []
  walker = lambda do |nn|
    if nn.key?(:err)
      msgs << [nn[:err].class.name, nn[:err].message, nn[:at]]
    else
      nn[:entries].each { |cc| walker.call(cc) }
    end
  end
  walker.call(mm)
  po.send(msgs)
end

w.send(report(5, 1))
back = port.receive
assert back.size == 32, "leaf count #{back.size}"
back.each do |cls, msgv, at|
  assert cls == "RuntimeError", "class"
  assert msgv == "leaf-#{at}", "message #{msgv} at #{at}"
end
assert back.map(&:last).sort == (32...64).to_a, "leaf ids"
puts "OK f55_exc_nested_payload"
