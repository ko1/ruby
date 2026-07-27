# Retry with deterministic logical backoff: failed job re-queued with ready_at =
# tick + 2**attempt; completion tick per job matches model. Axes: copy, stress svc.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
JOBS = Ractor.make_shareable((0...12).map { |i| { id: i, fails: i % 3 } })
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  pending = JOBS.map { |j| [j[:id], j[:fails], 0] } # id, fails_left, ready_at
  att = Hash.new(0)
  completed = {}
  tick = 0
  loop do
    msg = Ractor.receive
    break if msg == :stop
    rp = msg
    tick += 1
    run = pending.select { |_, _, ra| ra <= tick }
    pending -= run
    run.each do |id, fl, _|
      att[id] += 1
      if fl > 0
        pending << [id, fl - 1, tick + 2**att[id]]
      else
        completed[id] = tick
      end
    end
    rp << completed.size
  end
  GC.stress = false
  done << :done
  [completed, att.dup]
end
# model of the same tick machine
mp = JOBS.map { |j| [j[:id], j[:fails], 0] }
matt = Hash.new(0)
mcomp = {}
mt = 0
rp = Ractor::Port.new
40.times do
  mt += 1
  run = mp.select { |_, _, ra| ra <= mt }
  mp -= run
  run.each do |id, fl, _|
    matt[id] += 1
    if fl > 0
      mp << [id, fl - 1, mt + 2**matt[id]]
    else
      mcomp[id] = mt
    end
  end
  svc.send(rp)
  raise "tick#{mt}" unless rp.receive == mcomp.size
end
raise "model incomplete" unless mcomp.size == 12
svc.send(:stop)
done.receive
completed, att = svc.value
raise "completion ticks" unless completed == mcomp
JOBS.each { |j| raise "att#{j[:id]}" unless att[j[:id]] == j[:fails] + 1 }
puts "OK d44_jobq_backoff_ticks"
