# gen4 pipeline: bounded in-flight count. Source (main) only keeps WINDOW items
# in flight; the sink acks each item back to main through an ack port.
# axes: transfer=copy, GC=periodic GC.start in middle stage, exceptions=none, payload=hashes
N_ITEMS = 600
WINDOW = 8

ack = Ractor::Port.new
out = Ractor::Port.new

sink = Ractor.new(ack, out) do |a, o|
  n = sum = 0
  while (m = Ractor.receive) != :eos
    n += 1
    sum += m[:v]
    a << m[:id]
  end
  o << [n, sum]
end

mid = Ractor.new(sink) do |nxt|
  seen = 0
  while (m = Ractor.receive) != :eos
    seen += 1
    GC.start if seen % 150 == 0
    nxt << { id: m[:id], v: m[:v] * 2 }
  end
  nxt << :eos
end

expected = 0
inflight = 0
N_ITEMS.times do |i|
  if inflight >= WINDOW
    ack.receive
    inflight -= 1
  end
  expected += i * 2
  mid << { id: i, v: i }
  inflight += 1
end
mid << :eos
inflight.times { ack.receive }

n, sum = out.receive
[mid, sink].each(&:join)
raise "FAIL n #{n}" unless n == N_ITEMS
raise "FAIL sum #{sum} != #{expected}" unless sum == expected
puts "OK pl_backpressure"
