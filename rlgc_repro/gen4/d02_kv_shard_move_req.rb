# Sharded KV store: request bodies sent with move: (fresh strings/arrays each time).
# Axes: shards=3, 90 puts + 90 gets, move requests / copy responses, stress in services.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
N = 3
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
90.times do |i|
  body = Array.new(6) { |j| +"m#{i}-#{j}" }
  shards[i % N].send([:put, +"k#{i}", body, rp], move: true)
  raise "put#{i}" unless rp.receive == :ok
end
90.times do |i|
  shards[i % N].send([:get, +"k#{i}", nil, rp], move: true)
  v = rp.receive
  raise "get#{i}" unless v == Array.new(6) { |j| "m#{i}-#{j}" }
end
shards.each { _1.send(:stop) }
N.times { done.receive }
raise unless shards.map(&:value).sum == 90
puts "OK d02_kv_shard_move_req"
