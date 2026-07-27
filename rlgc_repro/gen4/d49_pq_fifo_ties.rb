# Stable priority queue: entries [prio, seq]; equal priorities pop FIFO.
# Axes: 90 entries / 5 prio levels, copy, stress in service, GC.start scattered.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  q = [] # kept sorted by [prio, seq]
  seq = 0
  loop do
    msg = Ractor.receive
    break if msg == :stop
    op, prio, tag, rp = msg
    case op
    when :push
      seq += 1
      ent = [prio, seq, tag]
      idx = q.bsearch_index { |e| (e <=> ent) > 0 } || q.size
      q.insert(idx, ent)
      rp << seq
    when :pop then rp << q.shift
    end
  end
  GC.stress = false
  done << :done
  q.size
end
rp = Ractor::Port.new
rng = Random.new(49)
pushed = []
90.times do |i|
  prio = rng.rand(5)
  pushed << [prio, i + 1, "t#{i}"]
  svc.send([:push, prio, "t#{i}", rp])
  raise unless rp.receive == i + 1
  GC.start if i % 40 == 39
end
model = pushed.sort_by { |p, s, _| [p, s] }
90.times do |i|
  svc.send([:pop, nil, nil, rp])
  got = rp.receive
  raise "pop#{i}: #{got.inspect}" unless got == model[i]
end
svc.send(:stop)
done.receive
raise unless svc.value == 0
puts "OK d49_pq_fifo_ties"
