# gen4 server-sim: session service where each handler keeps rich per-session
# objects (ivars + arrays + hashes) mutated across many requests; sessions
# expire (LRU) so old state becomes garbage while new requests arrive.
# axes: transfer=copy, GC=implicit churn + GC.start on expiry, exceptions=none
class Session
  attr_reader :id, :events, :attrs
  def initialize(id)
    @id = id
    @events = []
    @attrs = { created: true }
  end
  def touch(ev)
    @events << ev
    @events.shift if @events.size > 8
    @attrs[:last] = ev
  end
end

N_HANDLERS = 3
N_REQS = 450
MAX_SESSIONS = 12

handlers = N_HANDLERS.times.map do |hid|
  Ractor.new(hid) do |_id|
    sessions = {}
    expired = 0
    while (req = Ractor.receive) != :shutdown
      sid = req[:session]
      s = sessions[sid] ||= Session.new(sid)
      s.touch(req[:event])
      if sessions.size > MAX_SESSIONS
        old, = sessions.min_by { |_, ss| ss.attrs[:last] }
        sessions.delete(old)
        expired += 1
        GC.start if expired % 10 == 0
      end
      req[:reply] << [sid, s.events.size]
    end
    [sessions.size, expired]
  end
end

reply = Ractor::Port.new
N_REQS.times do |i|
  sid = "sess-#{i % 40}"
  handlers[sid.sum % N_HANDLERS] << { session: sid, event: i, reply: reply }
end

got = 0
N_REQS.times do
  _sid, nev = reply.receive
  raise "FAIL events" unless nev.between?(1, 8)
  got += 1
end
handlers.each { |h| h << :shutdown }
stats = handlers.map(&:value)
raise "FAIL live sessions" unless stats.all? { |live, _| live <= MAX_SESSIONS + 1 }
raise "FAIL count" unless got == N_REQS
puts "OK srv_session_state"
