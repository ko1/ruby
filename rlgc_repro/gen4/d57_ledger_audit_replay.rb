# Ledger + separate audit service: ledger forwards each applied op to the auditor,
# which independently replays; final balances must agree. Axes: 2 chained services,
# 140 ops, copy, stress in auditor, GC.compact in ledger loop.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
audit = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  bal = Hash.new(0)
  seen = 0
  loop do
    msg = Ractor.receive
    break if msg == :stop
    from, to, amt = msg
    bal[from] -= amt
    bal[to] += amt
    seen += 1
  end
  GC.stress = false
  done << :done
  [bal.dup, seen]
end
ledger = Ractor.new(audit, done) do |audit, done|
  bal = Hash.new(0)
  10.times { |i| bal["a#{i}"] = 200 }
  n = 0
  loop do
    msg = Ractor.receive
    break if msg == :stop
    from, to, amt, rp = msg
    n += 1
    GC.compact if n % 45 == 0
    if bal[from] >= amt
      bal[from] -= amt
      bal[to] += amt
      audit.send([from, to, amt])
      rp << :ok
    else
      rp << :rejected
    end
  end
  audit.send(:stop)
  done << :done
  bal.dup
end
rp = Ractor::Port.new
rng = Random.new(57)
applied = Hash.new(0)
napplied = 0
140.times do
  from = "a#{rng.rand(10)}"
  to = "a#{rng.rand(10)}"
  amt = rng.rand(1..80)
  ledger.send([from, to, amt, rp])
  if rp.receive == :ok
    applied[from] -= amt
    applied[to] += amt
    napplied += 1
  end
end
ledger.send(:stop)
2.times { done.receive }
lbal = ledger.value
abal, seen = audit.value
raise "audit count" unless seen == napplied
raise "conservation" unless lbal.values.sum == 2000
10.times do |i|
  a = "a#{i}"
  raise "agree #{a}" unless lbal[a] == 200 + abal[a] && abal[a] == applied[a]
end
puts "OK d57_ledger_audit_replay"
