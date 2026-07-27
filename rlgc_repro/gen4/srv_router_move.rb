# gen4 server-sim: request BODIES are large strings MOVED main->router->handler;
# handlers keep the last few bodies alive as per-key state and MOVE response
# buffers straight back to the client reply port.
# axes: transfer=move along the request path, GC=GC.start in handlers, exceptions=none
N_HANDLERS = 3
N_REQS = 240

handlers = N_HANDLERS.times.map do |hid|
  Ractor.new(hid) do |id|
    state = {}   # key => last body (moved-in string)
    n = 0
    while (req = Ractor.receive) != :shutdown
      key = req[:key]
      body = req[:body]
      state[key] = body            # retain moved-in object
      n += 1
      GC.start if n % 60 == 0
      resp = "resp:#{key}:#{body.size}"
      req[:reply].send(resp, move: true)
    end
    [n, state.size]
  end
end

router = Ractor.new(handlers, N_HANDLERS) do |hs, n|
  routed = 0
  while (req = Ractor.receive) != :shutdown
    hs[req[:key].sum % n].send(req, move: true)
    routed += 1
  end
  hs.each { |h| h << :shutdown }
  routed
end

reply = Ractor::Port.new
exp_sizes = 0
N_REQS.times do |i|
  key = "k#{i % 20}"
  body = "payload-#{i}|" + ("b" * (30 + i % 100))
  exp_sizes += body.size
  router.send({ key: key, body: body, reply: reply }, move: true)
end

got_sizes = 0
N_REQS.times do
  resp = reply.receive
  raise "FAIL resp #{resp[0, 10]}" unless resp.start_with?("resp:k")
  got_sizes += resp.split(":").last.to_i
end
router << :shutdown
raise "FAIL routed" unless router.value == N_REQS
stats = handlers.map(&:value)
raise "FAIL handled" unless stats.sum(&:first) == N_REQS
raise "FAIL state keys" unless stats.sum(&:last) == 20
raise "FAIL sizes" unless got_sizes == exp_sizes
puts "OK srv_router_move"
