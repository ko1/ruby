# Cron-like scheduler: entries fire every k ticks at offset o; 50 logical ticks;
# fired list equals model. Axes: 5 entries, copy, stress in service.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
CRON = Ractor.make_shareable([["e1", 3, 0], ["e2", 4, 1], ["e3", 5, 2], ["e4", 10, 5], ["e5", 7, 0]])
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  tick = 0
  fired = []
  loop do
    msg = Ractor.receive
    break if msg == :stop
    rp = msg
    tick += 1
    hits = CRON.select { |_, k, o| tick % k == o }.map(&:first)
    fired.concat(hits)
    rp << hits
  end
  GC.stress = false
  done << :done
  fired
end
rp = Ractor::Port.new
model = []
1.upto(50) do |t|
  hits = CRON.select { |_, k, o| t % k == o }.map(&:first)
  model.concat(hits)
  svc.send(rp)
  raise "tick#{t}" unless rp.receive == hits
end
svc.send(:stop)
done.receive
raise "fired" unless svc.value == model
raise "sanity" unless model.count("e1") == 16
puts "OK d39_sched_cron_ticks"
