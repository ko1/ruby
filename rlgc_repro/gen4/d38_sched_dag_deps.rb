# DAG scheduler: tasks with dependencies; service releases a task only after all
# deps completed; execution order respects topology and is deterministic.
# Axes: 12-task DAG, copy, stress in service, GC.compact between phases.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
DAG = Ractor.make_shareable({
  "a" => [], "b" => ["a"], "c" => ["a"], "d" => %w[b c], "e" => ["c"],
  "f" => %w[d e], "g" => [], "h" => %w[g f], "i" => ["h"], "j" => %w[i d],
  "k" => ["j"], "l" => %w[k g]
})
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  completed = {}
  order = []
  loop do
    msg = Ractor.receive
    break if msg == :stop
    op, rp = msg
    case op
    when :step
      ready = DAG.keys.sort.select { |t| !completed[t] && DAG[t].all? { completed[_1] } }
      ready.each { |t| completed[t] = true; order << t }
      rp << ready
    end
  end
  GC.stress = false
  done << :done
  order
end
rp = Ractor::Port.new
waves = []
loop do
  svc.send([:step, rp])
  w = rp.receive
  break if w.empty?
  waves << w
  GC.compact if waves.size == 2
end
raise "wave count" unless waves.size == 9
raise "wave1" unless waves[0] == %w[a g]
order = waves.flatten
raise "all run" unless order.sort == DAG.keys.sort
pos = order.each_with_index.to_h
DAG.each { |t, deps| deps.each { |d| raise "topo #{d}->#{t}" unless pos[d] < pos[t] } }
svc.send(:stop)
done.receive
raise unless svc.value == order
puts "OK d38_sched_dag_deps"
