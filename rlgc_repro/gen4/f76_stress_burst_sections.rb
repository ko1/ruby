# f76 stress bursts: GC.stress toggled ON around each send/receive section even without S_STRESS
# axes: copy+move alternating, bounded stress sections, GC.compact between bursts
Warning[:experimental] = false
STRESS = ENV['S_STRESS']

def assert(cond, msg = "assert")
  raise msg unless cond
end

port = Ractor::Port.new
w = Ractor.new(port) do |po|
  loop do
    mm = Ractor.receive
    break if mm == :eof
    po.send([mm[:n] * 2, mm[:s].upcase])
  end
end

bursts = STRESS ? 2 : 3
bursts.times do |b|
  GC.stress = true # burst on (regardless of env)
  if b.even?
    w.send({ n: b, s: "burst#{b}" })
  else
    obj = { n: b, s: +"burst#{b}" }
    w.send(obj, move: true)
    begin
      obj[:n]
      raise "not husked"
    rescue Ractor::MovedError
    end
  end
  dbl, up = port.receive
  GC.stress = false # burst off
  assert dbl == b * 2, "burst #{b} num"
  assert up == "BURST#{b}", "burst #{b} str"
  GC.compact
end
GC.stress = true if STRESS
w.send(:eof)
GC.start
puts "OK f76_stress_burst_sections"
