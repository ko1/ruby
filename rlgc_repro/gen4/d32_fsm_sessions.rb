# One service hosting many per-session FSMs (login flow); interleaved events over
# 15 sessions; final per-session states vs model. Axes: 180 events, copy, stress svc.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
STEP = Ractor.make_shareable({
  [:new, :hello] => :greeted, [:greeted, :auth] => :authed,
  [:authed, :work] => :authed, [:authed, :bye] => :closed,
  [:closed, :hello] => :greeted
})
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  fsms = Hash.new(:new)
  rejects = 0
  loop do
    msg = Ractor.receive
    break if msg == :stop
    sid, ev, rp = msg
    ns = STEP[[fsms[sid], ev]]
    if ns
      fsms[sid] = ns
      rp << ns
    else
      rejects += 1
      rp << :reject
    end
  end
  GC.stress = false
  done << :done
  [fsms, rejects]
end
rp = Ractor::Port.new
rng = Random.new(32)
mf = Hash.new(:new)
mrej = 0
evs = %i[hello auth work bye]
180.times do
  sid = "s#{rng.rand(15)}"
  ev = evs[rng.rand(4)]
  ns = STEP[[mf[sid], ev]]
  want = if ns
           mf[sid] = ns
         else
           mrej += 1
           :reject
         end
  svc.send([sid, ev, rp])
  raise "step" unless rp.receive == want
end
svc.send(:stop)
done.receive
fsms, rejects = svc.value
raise "fsm states" unless fsms == mf && rejects == mrej
puts "OK d32_fsm_sessions"
