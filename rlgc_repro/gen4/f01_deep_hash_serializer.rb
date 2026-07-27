# f01 serializer service: depth-6 nested Hash tree copy round-trip via port
# axes: copy, GC.start mid-flow, single worker request/response
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

def build(depth, fan)
  return { leaf: depth, tag: "v#{depth}", n: depth * 3 } if depth == 0
  hh = {}
  fan.times { |i| hh["k#{i}"] = build(depth - 1, fan) }
  hh
end

doc = build(STRESS ? 4 : 6, 2)
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  loop do
    mm = Ractor.receive
    break if mm == :eof
    po.send([mm, mm.size])
  end
end

rounds = STRESS ? 2 : 4
rounds.times do |i|
  w.send(doc)
  back, topn = port.receive
  assert back == doc, "round #{i}: deep equality failed"
  assert topn == 2, "top size"
  GC.start if i == 1
end
w.send(:eof)
puts "OK f01_deep_hash_serializer"
