# Sorted-insert priority queue with GC.compact every 10 service requests and
# string payloads; drain order exact. Axes: 80 items, copy, stress svc + compact.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  q = []
  n = 0
  loop do
    msg = Ractor.receive
    break if msg == :stop
    n += 1
    GC.compact if n % 10 == 0
    op, prio, name, rp = msg
    case op
    when :push
      ent = [prio, n, name]
      idx = q.bsearch_index { |e| (e <=> ent) > 0 } || q.size
      q.insert(idx, ent)
      rp << q.size
    when :pop then rp << q.shift
    end
  end
  GC.stress = false
  done << :done
  q.size
end
rp = Ractor::Port.new
rng = Random.new(51)
items = Array.new(80) { |i| [rng.rand(9), "task-#{i}-#{'x' * (i % 7)}"] }
items.each_with_index do |(p, name), i|
  svc.send([:push, p, name, rp])
  raise unless rp.receive == i + 1
end
model = items.each_with_index.map { |(p, name), i| [p, i + 1, name] }.sort
80.times do |i|
  svc.send([:pop, nil, nil, rp])
  raise "pop#{i}" unless rp.receive == model[i]
end
svc.send(:stop)
done.receive
raise unless svc.value == 0
puts "OK d51_pq_under_compact"
