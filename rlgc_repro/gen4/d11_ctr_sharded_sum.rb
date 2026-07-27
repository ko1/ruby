# Sharded counters: 4 shards, seeded increments; global sum conservation.
# Axes: shards=4, 240 incs, copy, stress in services, GC.start in client.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
N = 4
done = Ractor::Port.new
shards = N.times.map do
  Ractor.new(done, STRESS) do |done, stress|
    GC.stress = true if stress
    ctr = Hash.new(0)
    loop do
      msg = Ractor.receive
      break if msg == :stop
      op, name, by, rp = msg
      case op
      when :inc then ctr[name] += by; rp << :ok
      when :total then rp << ctr.values.sum
      end
    end
    GC.stress = false
    done << :done
    ctr.values.sum
  end
end
rp = Ractor::Port.new
rng = Random.new(11)
total = 0
240.times do |i|
  by = rng.rand(1..9)
  total += by
  shards[i % N].send([:inc, "c#{rng.rand(12)}", by, rp])
  raise unless rp.receive == :ok
  GC.start if i % 80 == 79
end
mid = shards.sum { |s| s.send([:total, nil, nil, rp]); rp.receive }
raise "mid #{mid} != #{total}" unless mid == total
shards.each { _1.send(:stop) }
N.times { done.receive }
raise "conservation" unless shards.map(&:value).sum == total
puts "OK d11_ctr_sharded_sum"
