# Job queue with deterministic retries: job i fails its first (i%4) attempts;
# total attempts per job == fails+1 exactly. Axes: 40 jobs, copy, stress svc.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  q = []
  attempts = Hash.new(0)
  donej = {}
  loop do
    msg = Ractor.receive
    break if msg == :stop
    op, job, rp = msg
    case op
    when :submit then q << job; rp << q.size
    when :drain
      until q.empty?
        j = q.shift
        attempts[j[:id]] += 1
        if attempts[j[:id]] <= j[:fails]
          q << j # retry at tail
        else
          donej[j[:id]] = attempts[j[:id]]
        end
      end
      rp << donej.size
    end
  end
  GC.stress = false
  done << :done
  [attempts.dup, donej]
end
rp = Ractor::Port.new
40.times do |i|
  svc.send([:submit, { id: i, fails: i % 4 }, rp])
  raise unless rp.receive == i + 1
end
svc.send([:drain, nil, rp])
raise unless rp.receive == 40
svc.send(:stop)
done.receive
attempts, donej = svc.value
40.times do |i|
  raise "attempts#{i}" unless attempts[i] == (i % 4) + 1 && donej[i] == (i % 4) + 1
end
raise "total attempts" unless attempts.values.sum == 40.times.sum { (_1 % 4) + 1 }
puts "OK d42_jobq_retry_counts"
