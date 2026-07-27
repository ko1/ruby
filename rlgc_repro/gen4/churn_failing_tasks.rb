# gen4 service+churn: continuous task churn where a third of tasks die with an
# exception; main reaps every task via #value with rescue while a service
# keeps serving the survivors.
# axes: transfer=copy, GC=none, exceptions=task death (reaped), lifecycle=churn
N_TASKS = 180

acc = Ractor.new do
  sum = 0
  while (m = Ractor.receive) != :shutdown
    sum += m
  end
  sum
end

ok = failed = 0
exp_sum = 0
(N_TASKS / 30).times do |wave|
  tasks = 30.times.map do |t|
    tid = wave * 30 + t
    Ractor.new(acc, tid) do |svc, id|
      Thread.current.report_on_exception = false
      raise "task #{id} exploded" if id % 3 == 1
      svc << id
      id
    end
  end
  tasks.each_with_index do |task, t|
    tid = wave * 30 + t
    begin
      v = task.value
      ok += 1
      exp_sum += v
    rescue Ractor::RemoteError => e
      raise "wrong msg" unless e.cause.message == "task #{tid} exploded"
      failed += 1
    end
  end
end

acc << :shutdown
got_sum = acc.value
exp_ok = (0...N_TASKS).count { |i| i % 3 != 1 }
raise "FAIL ok #{ok}" unless ok == exp_ok
raise "FAIL failed" unless failed == N_TASKS - exp_ok
raise "FAIL sum #{got_sum} != #{exp_sum}" unless got_sum == exp_sum
puts "OK churn_failing_tasks"
