# Turnstile FSM service (locked/unlocked; coin/push); pass count and final state
# must match a main-side replay. Axes: 200 seeded events, copy, stress in service.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  state = :locked
  passes = 0
  bounced = 0
  loop do
    msg = Ractor.receive
    break if msg == :stop
    ev, rp = msg
    case [state, ev]
    in [:locked, :coin] then state = :unlocked
    in [:locked, :push] then bounced += 1
    in [:unlocked, :coin] then nil
    in [:unlocked, :push] then state = :locked; passes += 1
    end
    rp << state
  end
  GC.stress = false
  done << :done
  [state, passes, bounced]
end
rp = Ractor::Port.new
rng = Random.new(29)
mstate = :locked
mpass = mbounce = 0
200.times do
  ev = rng.rand(2) == 0 ? :coin : :push
  if mstate == :locked
    ev == :coin ? mstate = :unlocked : mbounce += 1
  elsif ev == :push
    mstate = :locked
    mpass += 1
  end
  svc.send([ev, rp])
  raise "state" unless rp.receive == mstate
end
svc.send(:stop)
done.receive
raise "final" unless svc.value == [mstate, mpass, mbounce]
raise "sanity" unless mpass > 10 && mbounce > 10
puts "OK d29_fsm_turnstile"
