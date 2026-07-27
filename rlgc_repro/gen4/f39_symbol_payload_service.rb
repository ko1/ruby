# f39 telemetry keys: symbol-heavy payloads (static + dynamic to_sym), symbols pass by identity
# axes: copy, Symbol leaves, dynamic symbols, GC.start
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

port = Ractor::Port.new
w = Ractor.new(port) do |po|
  loop do
    mm = Ractor.receive
    break if mm == :eof
    counts = Hash.new(0)
    mm.each { |ev| counts[ev[:kind]] += 1 }
    po.send([counts, mm.first[:kind].equal?(:boot), mm.map { |ev| ev[:dyn] }])
  end
end

n = STRESS ? 6 : 24
events = n.times.map do |i|
  { kind: i.zero? ? :boot : [:tick, :warn, :io][i % 3], dyn: "dyn_#{i % 4}".to_sym }
end
w.send(events)
GC.start
counts, boot_identity, dyns = port.receive
assert counts.values.sum == n, "event count"
assert boot_identity, "static symbol identity across ractors"
assert dyns == n.times.map { |i| :"dyn_#{i % 4}" }, "dynamic symbols round-trip"
assert dyns[0].equal?(:dyn_0), "dynamic symbol identity"
w.send(:eof)
puts "OK f39_symbol_payload_service"
