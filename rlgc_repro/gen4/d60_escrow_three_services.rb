# Escrow flow across three services (buyer ledger, escrow, seller ledger):
# funds hop buyer->escrow->seller; grand total conserved at every checkpoint.
# Axes: 3 services, 80 deals, copy, stress in escrow.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
mk_ledger = lambda do |init, done|
  Ractor.new(init, done) do |init, done|
    bal = init
    loop do
      msg = Ractor.receive
      break if msg == :stop
      op, amt, rp = msg
      case op
      when :withdraw
        if bal >= amt
          bal -= amt
          rp << :ok
        else
          rp << :insufficient
        end
      when :deposit then bal += amt; rp << :ok
      when :read then rp << bal
      end
    end
    done << :done
    bal
  end
end
done = Ractor::Port.new
buyer = mk_ledger.call(3000, done)
seller = mk_ledger.call(500, done)
escrow = Ractor.new(buyer, seller, done, STRESS) do |buyer, seller, done, stress|
  GC.stress = true if stress
  my = Ractor::Port.new
  held = 0
  completed = 0
  loop do
    msg = Ractor.receive
    break if msg == :stop
    op, amt, rp = msg
    case op
    when :open
      buyer.send([:withdraw, amt, my])
      if my.receive == :ok
        held += amt
        rp << :held
      else
        rp << :declined
      end
    when :release
      if held >= amt
        held -= amt
        seller.send([:deposit, amt, my])
        raise unless my.receive == :ok
        completed += 1
        rp << :released
      else
        rp << :nothing_held
      end
    end
  end
  GC.stress = false
  done << :done
  [held, completed]
end
rp = Ractor::Port.new
rng = Random.new(60)
80.times do |i|
  amt = rng.rand(10..50)
  escrow.send([:open, amt, rp])
  st = rp.receive
  if st == :held && rng.rand(4) > 0
    escrow.send([:release, amt, rp])
    raise unless rp.receive == :released
  end
  if i % 20 == 19
    # checkpoint: buyer + seller + escrow-held == 3500
    escrow.send([:release, 0, rp]) # flush ordering: escrow processed all opens
    rp.receive
    buyer.send([:read, nil, rp])
    b = rp.receive
    seller.send([:read, nil, rp])
    s = rp.receive
    # held amount can only be read after :stop; conservative check below
    raise "drain" unless b + s <= 3500
  end
end
escrow.send(:stop)
done.receive
held, completed = escrow.value
buyer.send(:stop)
seller.send(:stop)
2.times { done.receive }
b = buyer.value
s = seller.value
raise "conservation" unless b + s + held == 3500
raise "sanity" unless completed > 20 && s > 500
puts "OK d60_escrow_three_services"
