# Replicated KV: every put goes to 3 replicas; reads from all replicas must agree.
# Axes: replicas=3, 60 keys, copy bodies, stress in replicas, GC.compact in client.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
R = 3
done = Ractor::Port.new
reps = R.times.map do
  Ractor.new(done, STRESS) do |done, stress|
    GC.stress = true if stress
    db = {}
    loop do
      msg = Ractor.receive
      break if msg == :stop
      op, k, v, rp = msg
      case op
      when :put then db[k] = v; rp << :ack
      when :get then rp << db[k]
      end
    end
    GC.stress = false
    done << :done
    db
  end
end
rp = Ractor::Port.new
60.times do |i|
  v = { id: i, payload: "data-#{i}" }
  reps.each { |r| r.send([:put, "k#{i}", v, rp]) }
  R.times { raise "ack#{i}" unless rp.receive == :ack }
  GC.compact if i % 25 == 24
end
20.times do |i|
  k = "k#{i * 3}"
  vals = reps.map { |r| r.send([:get, k, nil, rp]); rp.receive }
  raise "diverge #{k}" unless vals.uniq.size == 1 && vals[0][:id] == i * 3
end
reps.each { _1.send(:stop) }
R.times { done.receive }
dbs = reps.map(&:value)
raise "replica dbs differ" unless dbs.uniq.size == 1 && dbs[0].size == 60
puts "OK d03_kv_replicated"
