# gen4 server-sim: per-key counter service. Clients are themselves ractors
# issuing increments/reads against sharded counter handlers via a router;
# each client owns a private reply port.
# axes: transfer=copy, GC=none, exceptions=none, payload=small tuples
N_SHARDS = 3
N_CLIENTS = 6
OPS = 120

handlers = N_SHARDS.times.map do |sid|
  Ractor.new(sid) do |_id|
    counts = Hash.new(0)
    while (req = Ractor.receive) != :shutdown
      op, key, reply = req
      case op
      when :incr then counts[key] += 1
      when :read then reply << counts[key]
      end
    end
    counts.values.sum
  end
end

router = Ractor.new(handlers, N_SHARDS) do |hs, n|
  while (req = Ractor.receive) != :shutdown
    hs[req[1].sum % n] << req
  end
  hs.each { |h| h << :shutdown }
  :ok
end

clients = N_CLIENTS.times.map do |cid|
  Ractor.new(router, cid, OPS) do |rt, id, ops|
    reply = Ractor::Port.new
    ops.times { |i| rt << [:incr, "ctr:#{(id + i) % 10}", nil] }
    # read back one counter (value is a lower bound: other clients still run)
    rt << [:read, "ctr:#{id % 10}", reply]
    v = reply.receive
    raise "counter went missing" unless v >= 1
    ops
  end
end

total_ops = clients.sum(&:value)
router << :shutdown
router.join
grand = handlers.sum(&:value)
raise "FAIL ops" unless total_ops == N_CLIENTS * OPS
raise "FAIL grand #{grand}" unless grand == N_CLIENTS * OPS
puts "OK srv_counter"
