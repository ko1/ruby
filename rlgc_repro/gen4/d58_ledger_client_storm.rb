# Ledger under 4 concurrent client ractors doing seeded transfers; interleaving is
# nondeterministic but total balance conservation must hold. Axes: copy, stress
# in clients and service.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  bal = Hash.new(0)
  16.times { |i| bal["a#{i}"] = 125 }
  ops = 0
  loop do
    msg = Ractor.receive
    break if msg == :stop
    from, to, amt, rp = msg
    ops += 1
    if bal[from] >= amt
      bal[from] -= amt
      bal[to] += amt
      rp << :ok
    else
      rp << :rejected
    end
  end
  GC.stress = false
  done << :done
  [bal.values.sum, ops]
end
clients = 4.times.map do |ci|
  Ractor.new(svc, ci, done, STRESS) do |svc, ci, done, stress|
    GC.stress = true if stress
    my = Ractor::Port.new
    rng = Random.new(5800 + ci)
    oks = 0
    40.times do
      from = "a#{rng.rand(16)}"
      to = "a#{rng.rand(16)}"
      amt = rng.rand(1..50)
      svc.send([from, to, amt, my])
      oks += 1 if my.receive == :ok
    end
    GC.stress = false
    done << :cdone
    oks
  end
end
4.times { raise unless done.receive == :cdone }
oks = clients.map(&:value)
svc.send(:stop)
done.receive
total, ops = svc.value
raise "ops" unless ops == 160
raise "conservation #{total}" unless total == 16 * 125
raise "some succeeded" unless oks.sum > 40
puts "OK d58_ledger_client_storm"
