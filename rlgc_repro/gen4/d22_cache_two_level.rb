# Two-level cache: client ractor holds L1 hash; misses fall through to L2 service.
# Axes: 1 L2 service + 2 clients, deterministic L1 hit counts, copy, stress clients.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
l2 = Ractor.new(done) do |done|
  db = {}
  100.times { |i| db["k#{i}"] = "v#{i}" }
  gets = 0
  loop do
    msg = Ractor.receive
    break if msg == :stop
    k, rp = msg
    gets += 1
    rp << db[k]
  end
  done << :done
  gets
end
clients = 2.times.map do |ci|
  Ractor.new(l2, ci, done, STRESS) do |l2, ci, done, stress|
    GC.stress = true if stress
    my = Ractor::Port.new
    l1 = {}
    l1_hits = 0
    fetched = []
    # each key requested 3 times -> 1 L2 fetch + 2 L1 hits per key
    3.times do
      10.times do |i|
        k = "k#{ci * 10 + i}"
        if l1.key?(k)
          l1_hits += 1
          v = l1[k]
        else
          l2.send([k, my])
          v = my.receive
          l1[k] = v
          fetched << k
        end
        raise "val #{k}" unless v == "v#{ci * 10 + i}"
      end
    end
    GC.stress = false
    done << :cdone
    [l1_hits, fetched.size]
  end
end
2.times { raise unless done.receive == :cdone }
clients.each { |c| raise "client" unless c.value == [20, 10] }
l2.send(:stop)
done.receive
raise "l2 gets" unless l2.value == 20
puts "OK d22_cache_two_level"
