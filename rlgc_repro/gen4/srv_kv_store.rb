# gen4 server-sim: sharded key-value store. A router ractor forwards requests
# to 4 shard handlers by key; handlers keep per-key state; every request
# carries a reply port. Graceful shutdown: router fans out :shutdown, handlers
# return stats via #value.
# axes: transfer=copy, GC=none, exceptions=none, payload=hash requests w/ reply ports
N_SHARDS = 4
N_REQS = 500

handlers = N_SHARDS.times.map do |sid|
  Ractor.new(sid) do |id|
    store = {}
    gets = puts_ = 0
    while (req = Ractor.receive) != :shutdown
      case req[:op]
      when :put
        store[req[:key]] = req[:val]
        puts_ += 1
        req[:reply] << [:ok, req[:seq]]
      when :get
        gets += 1
        req[:reply] << [:val, req[:seq], store[req[:key]]]
      end
    end
    [id, store.size, gets, puts_]
  end
end

router = Ractor.new(handlers, N_SHARDS) do |hs, n|
  routed = 0
  while (req = Ractor.receive) != :shutdown
    shard = req[:key].sum % n     # String#sum: stable across ractors
    hs[shard] << req
    routed += 1
  end
  hs.each { |h| h << :shutdown }
  routed
end

reply = Ractor::Port.new
# phase 1: put keys
n_keys = 100
n_keys.times do |k|
  router << { op: :put, seq: k, key: "user:#{k}", val: "name-#{k}", reply: reply }
end
oks = 0
n_keys.times { tag, = reply.receive; oks += 1 if tag == :ok }
raise "FAIL puts" unless oks == n_keys

# phase 2: interleaved gets and overwrites
expected = {}
n_keys.times { |k| expected["user:#{k}"] = "name-#{k}" }
(N_REQS - n_keys).times do |i|
  k = "user:#{i % n_keys}"
  if i % 5 == 0
    v = "name-#{i}x"
    expected[k] = v
    router << { op: :put, seq: 1000 + i, key: k, val: v, reply: reply }
  else
    router << { op: :get, seq: 1000 + i, key: k, reply: reply }
  end
end
vals = 0
(N_REQS - n_keys).times do
  tag, _seq, v = reply.receive
  vals += 1 if tag == :val && v&.start_with?("name-")
end

router << :shutdown
routed = router.value
stats = handlers.map(&:value)
raise "FAIL routed #{routed}" unless routed == N_REQS
raise "FAIL keys" unless stats.sum { |s| s[1] } == n_keys
raise "FAIL gets" unless stats.sum { |s| s[2] } == (N_REQS - n_keys) * 4 / 5
raise "FAIL vals #{vals}" unless vals == (N_REQS - n_keys) * 4 / 5
puts "OK srv_kv_store"
