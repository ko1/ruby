# f12 job-ticket app: Struct instances carrying extra ivars moved to executor
# axes: move, Struct+ivars, husk assert, GC.start
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

Ticket = Struct.new(:id, :steps)

port = Ractor::Port.new
w = Ractor.new(port) do |po|
  loop do
    mm = Ractor.receive
    break if mm == :eof
    GC.start
    po.send([mm.id, mm.steps, mm.instance_variable_get(:@priority), mm.instance_variable_get(:@trace)])
  end
end

rounds = STRESS ? 2 : 5
rounds.times do |i|
  t = Ticket.new(100 + i, [:fetch, :parse, :store])
  t.instance_variable_set(:@priority, i.odd? ? :high : :low)
  t.instance_variable_set(:@trace, ["created-#{i}"])
  w.send(t, move: true)
  begin
    t.id
    raise "ticket not husked"
  rescue Ractor::MovedError
  end
  tid, steps, prio, trace = port.receive
  assert tid == 100 + i, "id"
  assert steps == [:fetch, :parse, :store], "steps"
  assert prio == (i.odd? ? :high : :low), "ivar priority"
  assert trace == ["created-#{i}"], "ivar trace"
end
w.send(:eof)
puts "OK f12_struct_ivar_move"
