# Scheduler with cancellation: register 30 one-shot jobs at future ticks, cancel
# a third before firing; fired set excludes cancelled. Axes: copy, stress svc.
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
    op, name, at, rp = msg
    case op
    when :at then jobs[name] = at; rp << :ok
    when :cancel then rp << (jobs.delete(name) ? :cancelled : :missing)
    when :tick
      tick += 1
      due = jobs.select { |_, t| t == tick }.keys.sort
      due.each { |n| jobs.delete(n); fired << n }
      rp << due
    end
  end
  GC.stress = false
  done << :done
  fired
end
rp = Ractor::Port.new
30.times do |i|
  svc.send([:at, "job#{i}", (i % 10) + 1, rp])
  raise unless rp.receive == :ok
end
cancelled = []
30.times do |i|
  next unless i % 3 == 0
  svc.send([:cancel, "job#{i}", nil, rp])
  raise unless rp.receive == :cancelled
  cancelled << "job#{i}"
end
svc.send([:cancel, "nope", nil, rp])
raise unless rp.receive == :missing
model_fired = []
1.upto(10) do |t|
  due = (0...30).select { |i| (i % 10) + 1 == t && i % 3 != 0 }.map { "job#{_1}" }.sort
  model_fired.concat(due)
  svc.send([:tick, nil, nil, rp])
  raise "tick#{t}" unless rp.receive == due
end
svc.send(:stop)
done.receive
fired = svc.value
raise "fired" unless fired == model_fired
raise "cancelled leaked" unless (fired & cancelled).empty?
raise "count" unless fired.size == 20
puts "OK d41_sched_cancel"
