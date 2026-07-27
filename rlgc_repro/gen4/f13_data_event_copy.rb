# f13 event bus: Data.define events copied to two subscribers, value equality
# axes: copy, Data (frozen), fan-out, GC.compact mid
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

Event = Data.define(:kind, :seq, :payload)

port = Ractor::Port.new
subs = 2.times.map do |wi|
  Ractor.new(port, wi) do |po, myid|
    loop do
      mm = Ractor.receive
      break if mm == :eof
      po.send([myid, mm.kind, mm.seq, mm.payload, mm.frozen?])
    end
  end
end

n = STRESS ? 2 : 5
n.times do |i|
  ev = Event.new(kind: :tick, seq: i, payload: { at: i * 100, src: "bus" })
  subs.each { |s| s.send(ev) }
  GC.compact if i == 1
  2.times do
    wid, kind, seq, pl, fz = port.receive
    assert kind == :tick && seq == i, "sub #{wid} event fields"
    assert pl == { at: i * 100, src: "bus" }, "sub #{wid} payload"
    assert fz, "Data copy should stay frozen"
  end
end
subs.each { |s| s.send(:eof) }
puts "OK f13_data_event_copy"
