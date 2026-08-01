# star: 4 spokes each push one big nested graph (hash of arrays of structs) to hub, hub checksums
# axes: big copied payload fan-in, Struct in nested container, GC.compact at hub after intake
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

Cell = Struct.new(:a, :b)
def bigpayload(id)
  h = {}
  5.times { |i| h["k#{id}_#{i}"] = 8.times.map { |j| Cell.new(id * 100 + i, j) } }
  h
end

def pck(h)
  h.sum { |k, cells| k.length + cells.sum { |c| c.a + c.b } }
end

K = 4
hreg = Ractor::Port.new
hub = Ractor.new(hreg, K) do |reg, k|
  inbox = Ractor::Port.new
  reg.send(inbox)
  total = 0
  k.times do
    id, payload = inbox.receive
    total += pck(payload) - id
  end
  GC.compact
  total
end
hub_in = hreg.receive
spokes = K.times.map do |i|
  Ractor.new(i, hub_in) do |id, h|
    h.send([id, bigpayload(id)])
    :fin
  end
end
exp = (0...K).sum { |i| pck(bigpayload(i)) - i }
GC.stress = false
raise "hub sum" unless hub.value == exp
spokes.each { |r| raise unless r.value == :fin }
puts "OK e24_star_bignest"
