# Payload zoo: echo/annotate service round-trips Hash, Array, Struct, ivar'd
# object, and frozen-shareable graph; shareable identity preserved.
# Axes: 5 payload kinds x 12 rounds, copy + shareable ref, stress in service.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
Pt = Struct.new(:x, :y)
class Box
  attr_reader :a, :b
  def initialize(a, b) = (@a = a; @b = b)
end
FROZEN = Ractor.make_shareable({ cfg: [1, 2, 3].freeze, name: "zoo" })
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  n = 0
  loop do
    msg = Ractor.receive
    break if msg == :stop
    kind, payload, rp = msg
    n += 1
    case kind
    when :hash then rp << payload.merge(seen: n)
    when :array then rp << (payload + [n])
    when :struct then rp << Pt.new(payload.x + 1, payload.y + 1)
    when :ivar then rp << Box.new(payload.b, payload.a) # swapped
    when :frozen
      raise "not same" unless payload.equal?(FROZEN)
      rp << payload
    end
  end
  GC.stress = false
  done << :done
  n
end
rp = Ractor::Port.new
12.times do |i|
  svc.send([:hash, { i: i }, rp])
  raise "hash#{i}" unless rp.receive == { i: i, seen: i * 5 + 1 }
  svc.send([:array, ["a", i], rp])
  raise "arr#{i}" unless rp.receive == ["a", i, i * 5 + 2]
  svc.send([:struct, Pt.new(i, -i), rp])
  raise "struct#{i}" unless rp.receive == Pt.new(i + 1, -i + 1)
  svc.send([:ivar, Box.new("L#{i}", "R#{i}"), rp])
  got = rp.receive
  raise "ivar#{i}" unless got.a == "R#{i}" && got.b == "L#{i}"
  svc.send([:frozen, FROZEN, rp])
  raise "frozen#{i}" unless rp.receive.equal?(FROZEN)
end
svc.send(:stop)
done.receive
raise unless svc.value == 60
puts "OK d77_payload_zoo"
