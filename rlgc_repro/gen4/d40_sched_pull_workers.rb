# Pull-based dispatch: workers request :next from a queue service via their own
# ports; total processed conserved; each task done exactly once.
# Axes: 3 workers, 90 tasks, copy, stress in workers.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
queue = Ractor.new(done) do |done|
  tasks = (0...90).map { |i| [i, i * 3] }
  handed = 0
  loop do
    msg = Ractor.receive
    break if msg == :stop
    rp = msg
    if tasks.empty?
      rp << :empty
    else
      handed += 1
      rp << tasks.shift
    end
  end
  done << :done
  handed
end
workers = 3.times.map do |wi|
  Ractor.new(queue, wi, done, STRESS) do |queue, wi, done, stress|
    GC.stress = true if stress
    my = Ractor::Port.new
    got = []
    loop do
      queue.send(my)
      t = my.receive
      break if t == :empty
      tid, x = t
      raise "calc" unless x == tid * 3
      got << tid
    end
    GC.stress = false
    done << :cdone
    got
  end
end
3.times { raise unless done.receive == :cdone }
lists = workers.map(&:value)
all = lists.flatten.sort
raise "coverage" unless all == (0...90).to_a
queue.send(:stop)
done.receive
raise "handed" unless queue.value == 90
puts "OK d40_sched_pull_workers"
