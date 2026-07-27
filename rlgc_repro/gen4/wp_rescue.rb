# gen4 worker-pool: some jobs are malformed and raise inside the worker; the
# worker rescues, reports an error record, and keeps serving. Pool survives.
# axes: transfer=copy, GC=occasional GC.start, exceptions=raised+rescued in workers, payload=mixed
N_WORKERS = 4
N_JOBS = 280

results = Ractor::Port.new
workers = N_WORKERS.times.map do |wid|
  Ractor.new(results, wid) do |res, id|
    ok = err = 0
    while (job = Ractor.receive) != :stop
      begin
        val = 100 / job[:den] + Integer(job[:num])
        ok += 1
        res << [:ok, job[:seq], val]
      rescue ZeroDivisionError, ArgumentError, TypeError => e
        err += 1
        res << [:err, job[:seq], e.class.name]
      end
      GC.start if (ok + err) % 70 == 0
    end
    [ok, err]
  end
end

exp_ok = exp_err = 0
N_JOBS.times do |i|
  job =
    case i % 7
    when 3 then { seq: i, den: 0, num: "1" }          # ZeroDivisionError
    when 5 then { seq: i, den: 4, num: "not-a-num" }  # ArgumentError
    else        { seq: i, den: 1 + i % 9, num: i.to_s }
    end
  if i % 7 == 3 || i % 7 == 5 then exp_err += 1 else exp_ok += 1 end
  workers[i % N_WORKERS] << job
end
workers.each { |w| w << :stop }

got_ok = got_err = 0
N_JOBS.times do
  tag, = results.receive
  tag == :ok ? got_ok += 1 : got_err += 1
end
sums = workers.map(&:value)
raise "FAIL ok #{got_ok}/#{exp_ok}" unless got_ok == exp_ok && sums.sum(&:first) == exp_ok
raise "FAIL err #{got_err}/#{exp_err}" unless got_err == exp_err && sums.sum(&:last) == exp_err
puts "OK wp_rescue"
