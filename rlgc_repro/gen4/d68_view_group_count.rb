# Group-by count view: events tagged with category; view keeps per-category counts
# incrementally; equals model tally. Axes: 200 events, copy, stress in service,
# GC.compact scattered.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
view = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  counts = Hash.new(0)
  n = 0
  loop do
    msg = Ractor.receive
    break if msg == :stop
    op, cat, rp = msg
    case op
    when :event
      counts[cat] += 1
      n += 1
      GC.compact if n % 64 == 0
    when :snapshot then rp << counts.dup
    end
  end
  GC.stress = false
  done << :done
  counts.dup
end
rp = Ractor::Port.new
rng = Random.new(68)
cats = %w[alpha beta gamma delta epsilon]
model = Hash.new(0)
200.times do |i|
  c = cats[rng.rand(5)]
  model[c] += 1
  view.send([:event, c, nil])
  if i % 50 == 49
    view.send([:snapshot, nil, rp])
    raise "snap@#{i}" unless rp.receive == model
  end
end
view.send(:stop)
done.receive
final = view.value
raise "final" unless final == model && final.values.sum == 200
puts "OK d68_view_group_count"
