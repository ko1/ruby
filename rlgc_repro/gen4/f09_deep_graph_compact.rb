# f09 snapshot service: depth-6 graph copy rounds with GC.compact on both sides
# axes: copy, GC.compact per round, send-die-value avoided (ports only)
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

def snap(depth, salt)
  return { s: "leaf-#{salt}", f: salt * 1.25 } if depth == 0
  { d: depth, kids: [snap(depth - 1, salt), snap(depth - 1, salt + 1)] }
end

port = Ractor::Port.new
w = Ractor.new(port) do |po|
  loop do
    mm = Ractor.receive
    break if mm == :eof
    GC.compact
    po.send(mm)
  end
end

rounds = STRESS ? 2 : 3
rounds.times do |i|
  doc = snap(6, i)
  w.send(doc)
  GC.compact
  back = port.receive
  assert back == doc, "round #{i}: compact broke round-trip"
end
w.send(:eof)
puts "OK f09_deep_graph_compact"
