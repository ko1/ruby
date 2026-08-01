# Append-only log with hash-chained records: chain[i] = (chain[i-1]*31 + rec) mod M;
# service and main model must agree at every 10th append and at replay.
# Axes: 100 appends, copy, stress in service.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
M = 1_000_000_007
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  log = []
  chain = 7
  loop do
    msg = Ractor.receive
    break if msg == :stop
    op, k, v, rp = msg
    case op
    when :append
      rec = (k.sum * 131 + v) % M
      chain = (chain * 31 + rec) % M
      log << [k, v, chain]
      rp << chain
    when :verify
      c = 7
      ok = log.all? do |lk, lv, lc|
        c = (c * 31 + (lk.sum * 131 + lv) % M) % M
        c == lc
      end
      rp << ok
    end
  end
  GC.stress = false
  done << :done
  [log.size, chain]
end
rp = Ractor::Port.new
rng = Random.new(63)
mchain = 7
100.times do |i|
  k = "rec#{rng.rand(30)}"
  v = rng.rand(1_000_000)
  mchain = (mchain * 31 + (k.sum * 131 + v) % M) % M
  svc.send([:append, k, v, rp])
  raise "chain#{i}" unless rp.receive == mchain
  if i % 10 == 9
    svc.send([:verify, nil, nil, rp])
    raise "verify@#{i}" unless rp.receive == true
  end
end
svc.send(:stop)
done.receive
n, chain = svc.value
raise unless n == 100 && chain == mchain
puts "OK d63_alog_checksum_chain"
