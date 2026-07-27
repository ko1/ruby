# Two account shards + coordinator ractor doing debit-then-credit transfers;
# cross-shard conservation. Axes: 2 shards + coordinator, 120 xfers, copy,
# stress in shards.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
shards = 2.times.map do |si|
  Ractor.new(si, done, STRESS) do |si, done, stress|
    GC.stress = true if stress
    bal = {}
    10.times { |i| bal["s#{si}a#{i}"] = 50 }
    loop do
      msg = Ractor.receive
      break if msg == :stop
      op, acct, amt, rp = msg
      case op
      when :debit
        if bal[acct] >= amt
          bal[acct] -= amt
          rp << :ok
        else
          rp << :insufficient
        end
      when :credit then bal[acct] += amt; rp << :ok
      end
    end
    GC.stress = false
    done << :done
    bal.values.sum
  end
end
coord = Ractor.new(shards, done) do |shards, done|
  my = Ractor::Port.new
  okc = rejc = 0
  loop do
    msg = Ractor.receive
    break if msg == :stop
    fs, facct, ts, tacct, amt, rp = msg
    shards[fs].send([:debit, facct, amt, my])
    if my.receive == :ok
      shards[ts].send([:credit, tacct, amt, my])
      raise "credit failed" unless my.receive == :ok
      okc += 1
      rp << :ok
    else
      rejc += 1
      rp << :insufficient
    end
  end
  done << :done
  [okc, rejc]
end
rp = Ractor::Port.new
rng = Random.new(54)
nok = nrej = 0
120.times do |i|
  fs = rng.rand(2)
  ts = 1 - fs
  facct = "s#{fs}a#{rng.rand(10)}"
  tacct = "s#{ts}a#{rng.rand(10)}"
  amt = rng.rand(1..40)
  coord.send([fs, facct, ts, tacct, amt, rp])
  r = rp.receive
  r == :ok ? nok += 1 : nrej += 1
end
coord.send(:stop)
done.receive
raise "coord counts" unless coord.value == [nok, nrej]
shards.each { _1.send(:stop) }
2.times { done.receive }
raise "conservation" unless shards.map(&:value).sum == 1000
puts "OK d54_ledger_shard_coord"
