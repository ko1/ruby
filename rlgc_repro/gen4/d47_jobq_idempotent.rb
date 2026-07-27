# Idempotent job queue: duplicate submissions (same idem key) are dropped;
# processed count == unique keys. Axes: 120 submissions / 40 keys, copy, stress svc.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  seen = {}
  processed = []
  dups = 0
  loop do
    msg = Ractor.receive
    break if msg == :stop
    key, payload, rp = msg
    if seen.key?(key)
      dups += 1
      rp << :duplicate
    else
      seen[key] = true
      processed << [key, payload * 2]
      rp << :accepted
    end
  end
  GC.stress = false
  done << :done
  [processed.size, dups, processed.sum { _1[1] }]
end
rp = Ractor::Port.new
rng = Random.new(47)
first = {}
naccept = ndup = 0
120.times do |i|
  k = "idem-#{rng.rand(40)}"
  if first.key?(k)
    ndup += 1
    want = :duplicate
  else
    first[k] = i
    naccept += 1
    want = :accepted
  end
  svc.send([k, i, rp])
  raise "sub#{i}" unless rp.receive == want
end
svc.send(:stop)
done.receive
np, nd, sum = svc.value
raise "counts" unless np == naccept && nd == ndup && np + nd == 120
raise "payload sum" unless sum == first.values.sum * 2
puts "OK d47_jobq_idempotent"
