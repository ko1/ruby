# Counter service; main client scatters GC.start/GC.compact between requests.
# Axes: 1 service, 120 incs, copy, stress in service, heavy client-side GC calls.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  n = 0
  loop do
    msg = Ractor.receive
    break if msg == :stop
    by, rp = msg
    n += by
    rp << n
  end
  GC.stress = false
  done << :done
  n
end
rp = Ractor::Port.new
run = 0
120.times do |i|
  run += i
  svc.send([i, rp])
  raise "at#{i}" unless rp.receive == run
  case i % 15
  when 5 then GC.start
  when 10 then GC.compact
  when 14 then GC.start(full_mark: false)
  end
end
svc.send(:stop)
done.receive
raise unless svc.value == 120.times.sum { _1 }
puts "OK d15_ctr_gc_scatter"
