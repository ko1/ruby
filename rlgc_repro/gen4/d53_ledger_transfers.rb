# Ledger service: 20 accounts x 100 units; 200 seeded transfers (rejected when
# insufficient); total balance conserved and equals model. Axes: copy, stress svc.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  bal = Hash.new { |h, k| h[k] = 100 }
  20.times { |i| bal["a#{i}"] }
  rejects = 0
  loop do
    msg = Ractor.receive
    break if msg == :stop
    op, from, to, amt, rp = msg
    case op
    when :xfer
      if bal[from] >= amt
        bal[from] -= amt
        bal[to] += amt
        rp << :ok
      else
        rejects += 1
        rp << :insufficient
      end
    when :balance then rp << bal[from]
    end
  end
  GC.stress = false
  done << :done
  [bal.to_a.to_h, rejects] # strip default_proc for cross-ractor copy
end
rp = Ractor::Port.new
mbal = Hash.new(100)
20.times { |i| mbal["a#{i}"] = 100 }
mrej = 0
rng = Random.new(53)
200.times do |i|
  from = "a#{rng.rand(20)}"
  to = "a#{rng.rand(20)}"
  amt = rng.rand(1..60)
  want = if mbal[from] >= amt
           mbal[from] -= amt
           mbal[to] += amt
           :ok
         else
           mrej += 1
           :insufficient
         end
  svc.send([:xfer, from, to, amt, rp])
  raise "xfer#{i}" unless rp.receive == want
end
svc.send([:balance, "a0", nil, nil, rp])
raise unless rp.receive == mbal["a0"]
svc.send(:stop)
done.receive
bal, rejects = svc.value
raise "conservation" unless bal.values.sum == 2000
raise "model" unless bal == mbal && rejects == mrej
puts "OK d53_ledger_transfers"
