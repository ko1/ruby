# Logical-clock scheduler: jobs registered with intervals; main sends 60 ticks;
# fired log matches model exactly (no wall time). Axes: copy, stress in service.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  jobs = {}
  fired = []
  tick = 0
  loop do
    msg = Ractor.receive
    break if msg == :stop
    op, a, b, rp = msg
    case op
    when :register then jobs[a] = b; rp << jobs.size
    when :tick
      tick += 1
      jobs.keys.sort.each { |name| fired << [tick, name] if tick % jobs[name] == 0 }
      rp << fired.size
    end
  end
  GC.stress = false
  done << :done
  fired
end
rp = Ractor::Port.new
jobs = { "j2" => 2, "j3" => 3, "j5" => 5, "j7" => 7 }
jobs.each_with_index do |(n, iv), i|
  svc.send([:register, n, iv, rp])
  raise unless rp.receive == i + 1
end
model = []
1.upto(60) do |t|
  jobs.keys.sort.each { |n| model << [t, n] if t % jobs[n] == 0 }
  svc.send([:tick, nil, nil, rp])
  raise "tick#{t}" unless rp.receive == model.size
end
svc.send(:stop)
done.receive
raise "fired log" unless svc.value == model
puts "OK d37_sched_logical_ticks"
