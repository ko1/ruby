# Single counter service, 3 client ractors x 40 increments; exact final total.
# Axes: clients=3, fire-and-forget incs + synced finish, stress both sides.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  total = 0
  loop do
    msg = Ractor.receive
    break if msg == :stop
    op, by, rp = msg
    case op
    when :inc then total += by
    when :read then rp << total
    end
  end
  GC.stress = false
  done << :done
  total
end
clients = 3.times.map do |ci|
  Ractor.new(svc, ci, done, STRESS) do |svc, ci, done, stress|
    GC.stress = true if stress
    my = Ractor::Port.new
    sent = 0
    40.times do |i|
      by = ci + 1
      svc.send([:inc, by, nil])
      sent += by
    end
    svc.send([:read, nil, my])
    my.receive # sync: all our incs processed (in-order per sender)
    GC.stress = false
    done << :cdone
    sent
  end
end
3.times { raise unless done.receive == :cdone }
sent = clients.map(&:value)
raise unless sent == [40, 80, 120]
svc.send(:stop)
done.receive
raise "total" unless svc.value == 240
puts "OK d16_ctr_multiclient_total"
