# gen4 service+churn: a long-lived collector service; short-lived tasks build
# medium result buffers and MOVE them to the collector as they die (moved
# objects must outlive their creator ractor's teardown).
# axes: transfer=move at task death, GC=GC.start in collector, lifecycle=churn
N_TASKS = 200

collector = Ractor.new(N_TASKS) do |n|
  total_len = 0
  count = 0
  n.times do
    buf = Ractor.receive
    total_len += buf.bytesize
    count += 1
    GC.start if count % 50 == 0
  end
  [count, total_len]
end

exp_len = 0
(N_TASKS / 25).times do |wave|
  tasks = 25.times.map do |t|
    tid = wave * 25 + t
    Ractor.new(collector, tid) do |col, id|
      buf = "result-#{id}:" + ("r" * (40 + id % 60))
      len = buf.bytesize
      col.send(buf, move: true)   # last act before death
      len
    end
  end
  exp_len += tasks.sum(&:value)
end

count, total_len = collector.value
raise "FAIL count" unless count == N_TASKS
raise "FAIL len #{total_len} != #{exp_len}" unless total_len == exp_len
puts "OK churn_move_results"
