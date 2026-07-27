# Double-entry ledger service: every journal entry debits one account and credits
# another; trial balance must always sum to zero. Axes: 160 entries, copy, stress svc.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  acct = Hash.new(0)
  journal = []
  loop do
    msg = Ractor.receive
    break if msg == :stop
    op, dr, cr, amt, rp = msg
    case op
    when :post
      acct[dr] += amt
      acct[cr] -= amt
      journal << [dr, cr, amt]
      rp << journal.size
    when :trial then rp << acct.values.sum
    end
  end
  GC.stress = false
  done << :done
  [acct.dup, journal.size, journal.sum { _1[2] }]
end
rp = Ractor::Port.new
names = %w[cash sales inventory ar ap equity]
rng = Random.new(56)
mac = Hash.new(0)
vol = 0
160.times do |i|
  dr = names[rng.rand(6)]
  cr = names[(names.index(dr) + 1 + rng.rand(5)) % 6]
  amt = rng.rand(1..500)
  mac[dr] += amt
  mac[cr] -= amt
  vol += amt
  svc.send([:post, dr, cr, amt, rp])
  raise unless rp.receive == i + 1
  if i % 40 == 39
    svc.send([:trial, nil, nil, nil, rp])
    raise "trial@#{i}" unless rp.receive == 0
  end
end
svc.send(:stop)
done.receive
acct, n, volume = svc.value
raise "zero sum" unless acct.values.sum == 0
raise "acct match" unless acct == mac
raise "volume" unless n == 160 && volume == vol
puts "OK d56_ledger_double_entry"
