# Event-sourced counter map: service applies commands and appends to log;
# main folds returned log independently and compares final state.
# Axes: 1 service, 150 commands, copy, stress in service, GC.compact client.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
APPLY = Ractor.shareable_lambda do |state, ev|
  op, k, n = ev
  case op
  when :add then state[k] = (state[k] || 0) + n
  when :del then state.delete(k)
  when :reset then state[k] = 0
  end
  state
end
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  state = {}
  log = []
  loop do
    msg = Ractor.receive
    break if msg == :stop
    ev, rp = msg
    log << ev
    APPLY.call(state, ev)
    rp << log.size
  end
  GC.stress = false
  done << :done
  [state, log]
end
rp = Ractor::Port.new
rng = Random.new(23)
150.times do |i|
  k = "k#{rng.rand(12)}"
  ev = case rng.rand(4)
       when 0, 1 then [:add, k, rng.rand(1..5)]
       when 2 then [:reset, k]
       else [:del, k]
       end
  svc.send([ev, rp])
  raise unless rp.receive == i + 1
  GC.compact if i % 70 == 69
end
svc.send(:stop)
done.receive
state, log = svc.value
raise "log size" unless log.size == 150
folded = log.inject({}) { |s, ev| APPLY.call(s, ev) }
raise "fold mismatch" unless folded == state
puts "OK d23_es_fold_replay"
