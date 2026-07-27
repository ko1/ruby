# Job queue with max_retries=3: jobs failing more than 3 times land in the DLQ
# with exactly 4 attempts. Axes: 36 jobs, copy, stress in service, compact in drain.
Warning[:experimental] = false
STRESS = !ENV['S_STRESS'].nil?
MAXR = 3
done = Ractor::Port.new
svc = Ractor.new(done, STRESS) do |done, stress|
  GC.stress = true if stress
  q = []
  att = Hash.new(0)
  ok = []
  dlq = []
  loop do
    msg = Ractor.receive
    break if msg == :stop
    op, job, rp = msg
    case op
    when :submit then q << job; rp << :ok
    when :drain
      n = 0
      until q.empty?
        j = q.shift
        att[j[:id]] += 1
        n += 1
        GC.compact if n % 20 == 0
        if att[j[:id]] <= j[:fails]
          if att[j[:id]] > MAXR
            dlq << j[:id]
          else
            q << j
          end
        else
          ok << j[:id]
        end
      end
      rp << [ok.size, dlq.size]
    end
  end
  GC.stress = false
  done << :done
  [ok.sort, dlq.sort, att.dup]
end
rp = Ractor::Port.new
# job i fails i%6 times: fails in {0..5}; >3 (i.e. 4,5) => DLQ
36.times do |i|
  svc.send([:submit, { id: i, fails: i % 6 }, rp])
  raise unless rp.receive == :ok
end
svc.send([:drain, nil, rp])
nok, ndlq = rp.receive
raise "sizes #{nok} #{ndlq}" unless nok == 24 && ndlq == 12
svc.send(:stop)
done.receive
ok, dlq, att = svc.value
raise "ok set" unless ok == (0...36).select { _1 % 6 <= 3 }
raise "dlq set" unless dlq == (0...36).select { _1 % 6 > 3 }
36.times do |i|
  want = [i % 6, MAXR].min + 1
  raise "att#{i}" unless att[i] == want
end
puts "OK d43_jobq_dead_letter"
