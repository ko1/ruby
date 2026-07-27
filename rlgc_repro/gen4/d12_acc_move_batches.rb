# Accumulator service consuming moved batches of Integers; running-sum replies.
# Axes: 1 service, 60 batches x 20 ints, move requests, stress in service.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  sum = 0
  cnt = 0
  loop do
    msg = Ractor.receive
    break if msg == :stop
    batch, rp = msg
    sum += batch.sum
    cnt += batch.size
    rp << [sum, cnt]
  end
  GC.stress = false
  done << :done
  [sum, cnt]
end
rp = Ractor::Port.new
esum = 0
60.times do |i|
  batch = Array.new(20) { |j| i * 100 + j }
  esum += batch.sum
  svc.send([batch, rp], move: true)
  s, c = rp.receive
  raise "b#{i}" unless s == esum && c == (i + 1) * 20
end
svc.send(:stop)
done.receive
raise unless svc.value == [esum, 1200]
puts "OK d12_acc_move_batches"
