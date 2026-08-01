# Vending machine FSM: coins in == price*dispensed + refunds + credit (money
# conservation). Axes: 150 seeded events, copy, stress in service.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
PRICE = 50
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  credit = 0
  coins_in = 0
  dispensed = 0
  refunded = 0
  loop do
    msg = Ractor.receive
    break if msg == :stop
    ev, arg, rp = msg
    case ev
    when :coin
      credit += arg
      coins_in += arg
      rp << credit
    when :buy
      if credit >= PRICE
        credit -= PRICE
        dispensed += 1
        rp << :vend
      else
        rp << :need_more
      end
    when :refund
      refunded += credit
      r = credit
      credit = 0
      rp << r
    end
  end
  GC.stress = false
  done << :done
  [coins_in, dispensed, refunded, credit]
end
rp = Ractor::Port.new
rng = Random.new(31)
150.times do
  case rng.rand(6)
  when 0, 1, 2 then svc.send([:coin, [10, 20, 50][rng.rand(3)], rp])
  when 3, 4 then svc.send([:buy, nil, rp])
  else svc.send([:refund, nil, rp])
  end
  rp.receive
end
svc.send(:stop)
done.receive
coins_in, dispensed, refunded, credit = svc.value
raise "conservation" unless coins_in == dispensed * PRICE + refunded + credit
raise "sanity" unless dispensed > 5 && coins_in > 500
puts "OK d31_fsm_vending"
