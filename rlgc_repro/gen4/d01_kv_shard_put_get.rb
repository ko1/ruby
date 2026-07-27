# Sharded KV store: 4 shard services, main as client, copy request/response bodies.
# Axes: shards=4, 120 puts + 120 gets, copy, stress in services, GC.start in client.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
N = 4
done = Ractor::Port.new
shards = N.times.map do
  Ractor.new(done, STRESS) do |done, stress|
    GC.stress = true if stress
    db = {}
    loop do
      msg = Ractor.receive
      break if msg == :stop
      op, k, v, rp = msg
      case op
      when :put then db[k] = v; rp << :ok
      when :get then rp << db[k]
      end
    end
    GC.stress = false
    done << :done
    db.size
  end
end
rp = Ractor::Port.new
120.times do |i|
  shards[i % N].send([:put, "key#{i}", [i, i * i, "v#{i}"], rp])
  raise "put#{i}" unless rp.receive == :ok
  GC.start if i % 40 == 39
end
120.times do |i|
  shards[i % N].send([:get, "key#{i}", nil, rp])
  v = rp.receive
  raise "get#{i}: #{v.inspect}" unless v == [i, i * i, "v#{i}"]
end
shards.each { _1.send(:stop) }
N.times { done.receive }
sizes = shards.map(&:value)
raise "sizes #{sizes}" unless sizes.sum == 120 && sizes.all? { _1 == 30 }
puts "OK d01_kv_shard_put_get"
