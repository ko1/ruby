# CQRS: writer service applies commands and forwards events to a materialized-view
# service (per-key sums); main queries both sides and compares.
# Axes: 2 chained services, 100 commands, copy, stress in writer.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
view = Ractor.new(done) do |done|
  sums = Hash.new(0)
  loop do
    msg = Ractor.receive
    break if msg == :stop
    op, k, d, rp = msg
    case op
    when :event then sums[k] += d
    when :query then rp << sums[k]
    end
  end
  done << :done
  sums
end
writer = Ractor.new(view, done, STRESS) do |view, done, stress|
  GC.stress = true if stress
  applied = 0
  loop do
    msg = Ractor.receive
    break if msg == :stop
    k, d, rp = msg
    applied += 1
    view.send([:event, k, d, nil])
    rp << applied
  end
  GC.stress = false
  done << :done
  applied
end
rp = Ractor::Port.new
model = Hash.new(0)
rng = Random.new(27)
100.times do |i|
  k = "acct#{rng.rand(6)}"
  d = rng.rand(1..9)
  model[k] += d
  writer.send([k, d, rp])
  raise unless rp.receive == i + 1
end
writer.send(:stop)
done.receive
raise unless writer.value == 100
# writer stopped -> all events already forwarded (in-order); now query view
model.each_key do |k|
  view.send([:query, k, nil, rp])
  raise "view #{k}" unless rp.receive == model[k]
end
view.send(:stop)
done.receive
raise "view final" unless view.value == model
puts "OK d27_cqrs_view"
