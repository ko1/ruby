# Top-3 leaderboard view maintained incrementally from score events; checked
# against full sort of model every 25 events. Axes: 150 events, copy, stress svc.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
view = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  scores = Hash.new(0)
  loop do
    msg = Ractor.receive
    break if msg == :stop
    op, player, pts, rp = msg
    case op
    when :score then scores[player] += pts
    when :top3
      rp << scores.sort_by { |p, s| [-s, p] }.first(3)
    end
  end
  GC.stress = false
  done << :done
  scores.dup
end
rp = Ractor::Port.new
rng = Random.new(69)
model = Hash.new(0)
150.times do |i|
  p = "p#{rng.rand(12)}"
  pts = rng.rand(1..20)
  model[p] += pts
  view.send([:score, p, pts, nil])
  if i % 25 == 24
    view.send([:top3, nil, nil, rp])
    want = model.sort_by { |pl, s| [-s, pl] }.first(3)
    raise "top3@#{i}" unless rp.receive == want
  end
end
view.send(:stop)
done.receive
raise "final" unless view.value == model
puts "OK d69_view_topk"
