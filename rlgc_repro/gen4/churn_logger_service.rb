# gen4 service+churn: one long-lived logger service; 300 short-lived task
# ractors are created/joined in waves, each logging a few lines to the service
# before returning a small result.
# axes: transfer=copy, GC=none, exceptions=none, lifecycle=heavy ractor churn
WAVES = 6
PER_WAVE = 50

logger = Ractor.new do
  lines = 0
  by_task = Hash.new(0)
  while (m = Ractor.receive) != :shutdown
    lines += 1
    by_task[m[:task]] += 1
  end
  [lines, by_task.size]
end

total = 0
WAVES.times do |w|
  tasks = PER_WAVE.times.map do |t|
    tid = w * PER_WAVE + t
    Ractor.new(logger, tid) do |log, id|
      3.times { |k| log << { task: id, line: "task #{id} step #{k}" } }
      id
    end
  end
  total += tasks.sum(&:value)
end

logger << :shutdown
lines, distinct = logger.value
n_tasks = WAVES * PER_WAVE
raise "FAIL ids" unless total == (0...n_tasks).sum
raise "FAIL lines #{lines}" unless lines == n_tasks * 3
raise "FAIL distinct" unless distinct == n_tasks
puts "OK churn_logger_service"
