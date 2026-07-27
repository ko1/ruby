# Join view over two source services (users, orders): view pulls both and joins
# user -> [name, order_total]; equals model join. Axes: 3 services, copy,
# stress in view.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
users = Ractor.new(done) do |done|
  tbl = {}
  loop do
    msg = Ractor.receive
    break if msg == :stop
    op, id, name, rp = msg
    case op
    when :put then tbl[id] = name; rp << :ok
    when :dump then rp << tbl.dup
    end
  end
  done << :done
  tbl.size
end
orders = Ractor.new(done) do |done|
  tot = Hash.new(0)
  loop do
    msg = Ractor.receive
    break if msg == :stop
    op, id, amt, rp = msg
    case op
    when :add then tot[id] += amt; rp << :ok
    when :dump then rp << tot.dup
    end
  end
  done << :done
  tot.size
end
view = Ractor.new(users, orders, done, STRESS) do |users, orders, done, stress|
  GC.stress = true if stress
  my = Ractor::Port.new
  joined = nil
  loop do
    msg = Ractor.receive
    break if msg == :stop
    rp = msg
    users.send([:dump, nil, nil, my])
    utbl = my.receive
    orders.send([:dump, nil, nil, my])
    otbl = my.receive
    joined = utbl.keys.sort.each_with_object({}) { |id, h| h[id] = [utbl[id], otbl[id] || 0] }
    rp << joined
  end
  GC.stress = false
  done << :done
  joined
end
rp = Ractor::Port.new
rng = Random.new(70)
munames = {}
mtot = Hash.new(0)
15.times do |i|
  munames["u#{i}"] = "name#{i}"
  users.send([:put, "u#{i}", "name#{i}", rp])
  raise unless rp.receive == :ok
end
100.times do
  id = "u#{rng.rand(15)}"
  amt = rng.rand(5..99)
  mtot[id] += amt
  orders.send([:add, id, amt, rp])
  raise unless rp.receive == :ok
end
view.send(rp)
j = rp.receive
model = munames.keys.sort.each_with_object({}) { |id, h| h[id] = [munames[id], mtot[id]] }
raise "join" unless j == model
view.send(:stop)
users.send(:stop)
orders.send(:stop)
3.times { done.receive }
raise unless view.value == model
puts "OK d70_view_join_two_sources"
