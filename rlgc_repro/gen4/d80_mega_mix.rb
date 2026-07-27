# Mega mix: sharded KV + double-entry ledger + incremental view service driven by
# one seeded op stream; all invariants checked at the end.
# Axes: 4 services, 150 mixed ops, copy, stress in kv+view, GC.compact scattered.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
kvs = 2.times.map do
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
ledger = Ractor.new(done) do |done|
  bal = Hash.new(0)
  loop do
    msg = Ractor.receive
    break if msg == :stop
    dr, cr, amt, rp = msg
    bal[dr] += amt
    bal[cr] -= amt
    rp << bal.values.sum
  end
  done << :done
  bal.to_a.to_h
end
view = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  counts = Hash.new(0)
  loop do
    msg = Ractor.receive
    break if msg == :stop
    kind = msg
    counts[kind] += 1
  end
  GC.stress = false
  done << :done
  counts.to_a.to_h
end
rp = Ractor::Port.new
rng = Random.new(80)
mdb = {}
mbal = Hash.new(0)
mcounts = Hash.new(0)
150.times do |i|
  case rng.rand(3)
  when 0
    k = "k#{rng.rand(30)}"
    v = [i, "v#{i}"]
    mdb[k] = v
    kvs[k.sum % 2].send([:put, k, v, rp])
    raise unless rp.receive == :ok
    view.send(:put)
    mcounts[:put] += 1
  when 1
    k = "k#{rng.rand(30)}"
    kvs[k.sum % 2].send([:get, k, nil, rp])
    raise "get#{i}" unless rp.receive == mdb[k]
    view.send(:get)
    mcounts[:get] += 1
  else
    dr = "acct#{rng.rand(5)}"
    cr = "acct#{rng.rand(5)}"
    amt = rng.rand(1..99)
    mbal[dr] += amt
    mbal[cr] -= amt
    ledger.send([dr, cr, amt, rp])
    raise "zero@#{i}" unless rp.receive == 0
    view.send(:post)
    mcounts[:post] += 1
  end
  GC.compact if i % 60 == 59
end
kvs.each { _1.send(:stop) }
ledger.send(:stop)
view.send(:stop)
4.times { done.receive }
raise "kv sizes" unless kvs.map(&:value).sum == mdb.size
raise "ledger" unless ledger.value == mbal.to_a.to_h
vc = view.value
raise "view" unless vc == mcounts.to_a.to_h && vc.values.sum == 150
puts "OK d80_mega_mix"
