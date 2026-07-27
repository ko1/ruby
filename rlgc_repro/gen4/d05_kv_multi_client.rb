# Sharded KV with 4 client ractors doing disjoint key ranges, verify per-client.
# Axes: shards=2, clients=4, 30 ops each, copy, stress in clients only.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
shards = 2.times.map do
  Ractor.new(done) do |done|
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
    done << :done
    db.size
  end
end
clients = 4.times.map do |ci|
  Ractor.new(shards, ci, done, STRESS) do |shards, ci, done, stress|
    GC.stress = true if stress
    my = Ractor::Port.new
    30.times do |i|
      k = "c#{ci}-#{i}"
      shards[i % 2].send([:put, k, { c: ci, i: i }, my])
      raise unless my.receive == :ok
    end
    ok = 0
    30.times do |i|
      k = "c#{ci}-#{i}"
      shards[i % 2].send([:get, k, nil, my])
      v = my.receive
      ok += 1 if v == { c: ci, i: i }
    end
    GC.stress = false
    done << :cdone
    ok
  end
end
4.times { raise unless done.receive == :cdone }
raise unless clients.map(&:value).all? { _1 == 30 }
shards.each { _1.send(:stop) }
2.times { raise unless done.receive == :done }
raise unless shards.map(&:value).sum == 120
puts "OK d05_kv_multi_client"
