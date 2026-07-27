# Queue service + 3 pulling workers; job i fails first i%3 attempts (worker
# reports :fail, queue re-enqueues); total attempts conserved.
# Axes: 45 jobs, copy, stress in workers.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
queue = Ractor.new(done) do |done|
  q = (0...45).map { |i| i }
  att = Hash.new(0)
  finished = 0
  loop do
    msg = Ractor.receive
    break if msg == :stop
    op, id, rp = msg
    case op
    when :next
      if q.empty?
        rp << (finished == 45 ? :empty : :wait)
      else
        j = q.shift
        att[j] += 1
        rp << [j, att[j]]
      end
    when :fail then q << id
    when :ok then finished += 1
    end
  end
  done << :done
  [att.dup, finished]
end
workers = 3.times.map do |wi|
  Ractor.new(queue, done, STRESS) do |queue, done, stress|
    GC.stress = true if stress
    my = Ractor::Port.new
    mine = 0
    loop do
      queue.send([:next, nil, my])
      j = my.receive
      break if j == :empty
      next if j == :wait
      id, attempt = j
      if attempt <= id % 3
        queue.send([:fail, id, nil])
      else
        queue.send([:ok, id, nil])
        mine += 1
      end
    end
    GC.stress = false
    done << :cdone
    mine
  end
end
3.times { raise unless done.receive == :cdone }
counts = workers.map(&:value)
raise "sum" unless counts.sum == 45
queue.send(:stop)
done.receive
att, finished = queue.value
raise unless finished == 45
raise "attempts" unless att.values.sum == 45.times.sum { (_1 % 3) + 1 }
45.times { |i| raise "att#{i}" unless att[i] == (i % 3) + 1 }
puts "OK d45_jobq_workers_retry"
