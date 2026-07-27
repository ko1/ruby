# Histogram service: buckets values 0..9 from seeded stream; compare with model.
# Axes: 1 service, 300 values in 30-item chunks, copy, stress in service, compact mid.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  hist = Array.new(10, 0)
  loop do
    msg = Ractor.receive
    break if msg == :stop
    vals, rp = msg
    vals.each { |v| hist[v] += 1 }
    rp << hist.sum
  end
  GC.stress = false
  done << :done
  hist
end
rp = Ractor::Port.new
rng = Random.new(13)
model = Array.new(10, 0)
10.times do |c|
  chunk = Array.new(30) { rng.rand(10) }
  chunk.each { |v| model[v] += 1 }
  svc.send([chunk, rp])
  raise "chunk#{c}" unless rp.receive == (c + 1) * 30
  GC.compact if c == 5
end
svc.send(:stop)
done.receive
hist = svc.value
raise "hist #{hist} != #{model}" unless hist == model && hist.sum == 300
puts "OK d13_hist_accumulator"
