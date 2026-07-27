# f60 event timeline: Time objects (local + utc) nested in payloads, copy fidelity
# axes: copy, Time leaves, arithmetic on copies, GC.compact
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

port = Ractor::Port.new
w = Ractor.new(port) do |po|
  mm = Ractor.receive
  GC.compact
  evs = mm[:events]
  span = evs.last[:t] - evs.first[:t]
  po.send([evs.map { |ev| [ev[:name], ev[:t].to_i, ev[:t].nsec, ev[:t].utc?] }, span])
end

base = Time.at(1_700_000_000, 123_456, :usec).utc
events = 4.times.map { |i| { name: :"ev#{i}", t: base + i * 60 } }
timeline = { events: events }
w.send(timeline)
GC.start
back, span = port.receive
4.times do |i|
  nm, ti, tn, tu = back[i]
  assert nm == :"ev#{i}", "name"
  assert ti == base.to_i + i * 60, "epoch ev#{i}"
  assert tn == 123_456_000, "nsec ev#{i} (#{tn})"
  assert tu, "utc flag ev#{i}"
end
assert span == 180.0, "span #{span}"
assert timeline[:events][0][:t].equal?(events[0][:t]), "source timeline intact"
puts "OK f60_time_copy_service"
