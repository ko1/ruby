# Inventory service with reserve/commit/rollback; invariant per SKU:
# on_hand + reserved + sold == initial stock at every checkpoint.
# Axes: 8 SKUs, 150 seeded ops, copy, stress in service, GC.start in client.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
INIT = 60
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  on = Hash.new(0)
  res = Hash.new(0)
  sold = Hash.new(0)
  8.times { |i| on["sku#{i}"] = INIT }
  loop do
    msg = Ractor.receive
    break if msg == :stop
    op, sku, n, rp = msg
    case op
    when :reserve
      if on[sku] >= n
        on[sku] -= n
        res[sku] += n
        rp << :reserved
      else
        rp << :nostock
      end
    when :commit
      if res[sku] >= n
        res[sku] -= n
        sold[sku] += n
        rp << :sold
      else
        rp << :noresv
      end
    when :rollback
      if res[sku] >= n
        res[sku] -= n
        on[sku] += n
        rp << :rolled
      else
        rp << :noresv
      end
    when :check
      rp << (on[sku] + res[sku] + sold[sku])
    end
  end
  GC.stress = false
  done << :done
  [on.dup, res.dup, sold.dup]
end
rp = Ractor::Port.new
rng = Random.new(55)
150.times do |i|
  sku = "sku#{rng.rand(8)}"
  n = rng.rand(1..5)
  op = %i[reserve reserve commit rollback][rng.rand(4)]
  svc.send([op, sku, n, rp])
  rp.receive
  if i % 25 == 24
    GC.start
    svc.send([:check, sku, nil, rp])
    raise "invariant@#{i}" unless rp.receive == INIT
  end
end
svc.send(:stop)
done.receive
on, res, sold = svc.value
8.times do |i|
  sku = "sku#{i}"
  raise "final #{sku}" unless on[sku] + res[sku] + sold[sku] == INIT
end
raise "total" unless on.values.sum + res.values.sum + sold.values.sum == 8 * INIT
puts "OK d55_inv_reserve_commit"
