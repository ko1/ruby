# 6 FSM ractors with different modulus params fed the same event stream; final
# states collected and compared to model. Axes: 6 services, 100 events, copy,
# stress in half the services.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
fsms = 6.times.map do |fi|
  Ractor.new(fi, done, STRESS) do |fi, done, stress|
    GC.stress = true if stress && fi.even?
    mod = 5 + fi
    st = 0
    seen = 0
    loop do
      msg = Ractor.receive
      break if msg == :stop
      st = (st + msg) % mod
      seen += 1
    end
    GC.stress = false
    done << :done
    [st, seen]
  end
end
rng = Random.new(35)
evs = Array.new(100) { rng.rand(1..9) }
evs.each { |e| fsms.each { |f| f.send(e) } }
fsms.each { |f| f.send(:stop) }
6.times { done.receive }
finals = fsms.map(&:value)
model = 6.times.map do |fi|
  mod = 5 + fi
  [evs.inject(0) { |s, e| (s + e) % mod }, 100]
end
raise "finals #{finals}" unless finals == model
puts "OK d35_fsm_army"
