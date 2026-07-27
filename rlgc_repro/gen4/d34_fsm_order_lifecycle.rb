# Order lifecycle FSM service: created->paid->shipped->delivered or cancel path;
# 40 orders with scripted flows; final bucket counts exact. Axes: copy, stress svc.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
FLOW = Ractor.make_shareable({
  [:created, :pay] => :paid, [:paid, :ship] => :shipped,
  [:shipped, :deliver] => :delivered, [:created, :cancel] => :cancelled,
  [:paid, :cancel] => :refunded
})
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  orders = {}
  loop do
    msg = Ractor.receive
    break if msg == :stop
    op, oid, ev, rp = msg
    case op
    when :create then orders[oid] = :created; rp << :created
    when :event
      ns = FLOW[[orders[oid], ev]]
      orders[oid] = ns if ns
      rp << (ns || :rejected)
    end
  end
  GC.stress = false
  done << :done
  orders.values.tally
end
rp = Ractor::Port.new
scripts = {
  %i[pay ship deliver] => :delivered,
  %i[pay cancel] => :refunded,
  %i[cancel] => :cancelled,
  %i[pay ship deliver cancel] => :delivered, # trailing cancel rejected
  %i[ship] => :created                        # ship before pay rejected
}
expect = Hash.new(0)
40.times do |i|
  script, final = scripts.to_a[i % scripts.size]
  expect[final] += 1
  oid = "o#{i}"
  svc.send([:create, oid, nil, rp])
  raise unless rp.receive == :created
  script.each { |ev| svc.send([:event, oid, ev, rp]); rp.receive }
  svc.send([:event, oid, :noop, rp])
  raise unless rp.receive == :rejected
end
svc.send(:stop)
done.receive
tally = svc.value
raise "tally #{tally}" unless tally == expect
puts "OK d34_fsm_order_lifecycle"
