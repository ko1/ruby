# f52 incident courier: custom exception subclass with ivars moved to triage worker
# axes: move, Exception+ivars, husk assert, GC.start
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

class IncidentError < StandardError
  attr_reader :code, :ctx
  def initialize(msg, code, ctx)
    super(msg)
    @code = code
    @ctx = ctx
  end
end

port = Ractor::Port.new
w = Ractor.new(port) do |po|
  loop do
    mm = Ractor.receive
    break if mm == :eof
    GC.start
    po.send([mm.class.name, mm.message, mm.code, mm.ctx])
  end
end

rounds = STRESS ? 2 : 4
rounds.times do |i|
  inc = IncidentError.new("disk full on node#{i}", 500 + i, { node: "n#{i}", retries: [1, 2, 3] })
  w.send(inc, move: true)
  begin
    inc.code
    raise "incident not husked"
  rescue Ractor::MovedError
  end
  cls, msgv, code, ctx = port.receive
  assert cls == "IncidentError", "class"
  assert msgv == "disk full on node#{i}", "message"
  assert code == 500 + i, "ivar code"
  assert ctx == { node: "n#{i}", retries: [1, 2, 3] }, "ivar ctx"
end
w.send(:eof)
puts "OK f52_exc_ivar_move"
