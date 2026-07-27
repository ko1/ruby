# WAL service over Hash-of-Array state (push/pop/clear per key); returned WAL
# folded by main must reproduce final state exactly. Axes: 150 ops, copy,
# stress in service, GC.start client.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  state = Hash.new { |h, k| h[k] = [] }
  wal = []
  loop do
    msg = Ractor.receive
    break if msg == :stop
    ev, rp = msg
    op, k, v = ev
    wal << ev
    case op
    when :push then state[k] << v
    when :pop then state[k].pop
    when :clear then state[k] = []
    end
    rp << state[k].size
  end
  GC.stress = false
  done << :done
  [state.to_a.to_h, wal] # strip default_proc for cross-ractor copy
end
rp = Ractor::Port.new
rng = Random.new(66)
150.times do |i|
  k = "q#{rng.rand(6)}"
  ev = case rng.rand(5)
       when 0, 1, 2 then [:push, k, i]
       when 3 then [:pop, k]
       else [:clear, k]
       end
  svc.send([ev, rp])
  rp.receive
  GC.start if i % 60 == 59
end
svc.send(:stop)
done.receive
state, wal = svc.value
raise "wal len" unless wal.size == 150
folded = Hash.new { |h, k| h[k] = [] }
wal.each do |op, k, v|
  case op
  when :push then folded[k] << v
  when :pop then folded[k].pop
  when :clear then folded[k] = []
  end
end
raise "fold" unless folded == state
puts "OK d66_wal_state_fold"
