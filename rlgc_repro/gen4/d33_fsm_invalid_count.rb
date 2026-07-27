# Door FSM (closed/open/locked) rejecting invalid transitions; exact reject count.
# Axes: 160 seeded events, copy, stress in service, GC.start in client.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
TRANS = Ractor.make_shareable({
  [:closed, :open] => :opened, [:opened, :close] => :closed,
  [:closed, :lock] => :locked, [:locked, :unlock] => :closed
})
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  st = :closed
  ok = bad = 0
  loop do
    msg = Ractor.receive
    break if msg == :stop
    ev, rp = msg
    ns = TRANS[[st, ev]]
    if ns
      st = ns
      ok += 1
      rp << st
    else
      bad += 1
      rp << :invalid
    end
  end
  GC.stress = false
  done << :done
  [st, ok, bad]
end
rp = Ractor::Port.new
rng = Random.new(33)
mst = :closed
mok = mbad = 0
evs = %i[open close lock unlock]
160.times do |i|
  ev = evs[rng.rand(4)]
  ns = TRANS[[mst, ev]]
  want = if ns
           mok += 1
           mst = ns
         else
           mbad += 1
           :invalid
         end
  svc.send([ev, rp])
  raise "ev#{i}" unless rp.receive == want
  GC.start if i % 55 == 54
end
raise "counts" unless mok + mbad == 160
svc.send(:stop)
done.receive
raise unless svc.value == [mst, mok, mbad]
puts "OK d33_fsm_invalid_count"
