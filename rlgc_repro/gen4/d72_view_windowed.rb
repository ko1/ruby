# Windowed aggregate view: logical windows of 10 ticks; per-window sums sealed on
# rollover and match model. Axes: 8 windows x 10 ticks, copy, stress in service.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
view = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  tick = 0
  cur = 0
  sealed = []
  loop do
    msg = Ractor.receive
    break if msg == :stop
    v, rp = msg
    tick += 1
    cur += v
    if tick % 10 == 0
      sealed << cur
      cur = 0
    end
    rp << sealed.size
  end
  GC.stress = false
  done << :done
  [sealed, cur]
end
rp = Ractor::Port.new
rng = Random.new(72)
msealed = []
mcur = 0
80.times do |t|
  v = rng.rand(1..100)
  mcur += v
  if (t + 1) % 10 == 0
    msealed << mcur
    mcur = 0
  end
  view.send([v, rp])
  raise "t#{t}" unless rp.receive == msealed.size
end
view.send(:stop)
done.receive
sealed, cur = view.value
raise "windows" unless sealed == msealed && sealed.size == 8 && cur == 0
puts "OK d72_view_windowed"
