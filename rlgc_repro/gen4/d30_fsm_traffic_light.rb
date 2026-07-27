# Traffic-light FSM: tick-driven G->Y->R->G with per-state dwell; counts per state
# after N ticks are exact. Axes: 180 ticks, copy, stress in service, compact mid.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
DWELL = Ractor.make_shareable({ green: 5, yellow: 2, red: 3 })
NEXT = Ractor.make_shareable({ green: :yellow, yellow: :red, red: :green })
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  state = :green
  left = DWELL[:green]
  counts = Hash.new(0)
  loop do
    msg = Ractor.receive
    break if msg == :stop
    rp = msg
    counts[state] += 1
    left -= 1
    if left == 0
      state = NEXT[state]
      left = DWELL[state]
    end
    rp << state
  end
  GC.stress = false
  done << :done
  counts
end
rp = Ractor::Port.new
ms = :green
ml = 5
mc = Hash.new(0)
180.times do |i|
  mc[ms] += 1
  ml -= 1
  if ml == 0
    ms = NEXT[ms]
    ml = DWELL[ms]
  end
  svc.send(rp)
  raise "tick#{i}" unless rp.receive == ms
  GC.compact if i == 90
end
svc.send(:stop)
done.receive
counts = svc.value
raise "counts" unless counts == mc
raise "cycle" unless counts[:green] == 90 && counts[:yellow] == 36 && counts[:red] == 54
puts "OK d30_fsm_traffic_light"
