# Streaming stats service (min/max/sum/count) fed by 2 client ractors, ranges disjoint.
# Axes: clients=2, 80 values each, copy, stress in clients, final stats deterministic.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
svc = Ractor.new(done) do |done|
  min = nil; max = nil; sum = 0; cnt = 0
  loop do
    msg = Ractor.receive
    break if msg == :stop
    v, rp = msg
    min = v if min.nil? || v < min
    max = v if max.nil? || v > max
    sum += v; cnt += 1
    rp << cnt
  end
  done << :done
  [min, max, sum, cnt]
end
clients = 2.times.map do |ci|
  Ractor.new(svc, ci, done, STRESS) do |svc, ci, done, stress|
    GC.stress = true if stress
    my = Ractor::Port.new
    80.times do |i|
      svc.send([ci * 1000 + i, my])
      my.receive
    end
    GC.stress = false
    done << :cdone
    :ok
  end
end
2.times { raise unless done.receive == :cdone }
clients.each { raise unless _1.value == :ok }
svc.send(:stop)
done.receive
min, max, sum, cnt = svc.value
raise "stats" unless min == 0 && max == 1079 && cnt == 160
raise "sum" unless sum == 80.times.sum { _1 } + 80.times.sum { 1000 + _1 }
puts "OK d14_stats_minmax"
