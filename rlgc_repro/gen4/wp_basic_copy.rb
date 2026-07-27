# gen4 worker-pool: fixed pool of 4 workers; dispatcher (main) pushes jobs
# round-robin to worker default ports; results gathered via one result port.
# axes: transfer=copy, GC=none, exceptions=none, payload=mixed small (str/arr/hash/pair)
N_WORKERS = 4
N_JOBS = 400

results = Ractor::Port.new
workers = N_WORKERS.times.map do |wid|
  Ractor.new(results, wid, name: "wp-#{wid}") do |res, id|
    processed = 0
    while (job = Ractor.receive) != :stop
      val =
        case job[:kind]
        when :str  then job[:body].upcase.length
        when :arr  then job[:body].sum
        when :hash then job[:body].values.sum
        when :pair then job[:body][0] + job[:body][1].size
        end
      processed += 1
      res << [id, job[:seq], val]
    end
    processed
  end
end

mkjob = lambda do |i|
  case i % 4
  when 0 then { kind: :str,  seq: i, body: "job-#{i}-" * (1 + i % 5) }
  when 1 then { kind: :arr,  seq: i, body: [i, i * 2, i * 3] }
  when 2 then { kind: :hash, seq: i, body: { a: i, b: i + 1, c: i + 2 } }
  else        { kind: :pair, seq: i, body: [i, "x" * (i % 7)] }
  end
end

expected = 0
N_JOBS.times do |i|
  job = mkjob.call(i)
  expected +=
    case job[:kind]
    when :str  then job[:body].length
    when :arr  then job[:body].sum
    when :hash then job[:body].values.sum
    when :pair then job[:body][0] + job[:body][1].size
    end
  workers[i % N_WORKERS] << job
end
workers.each { |w| w << :stop }

got = 0
seen = Array.new(N_JOBS, false)
N_JOBS.times do
  _id, seq, val = results.receive
  raise "dup seq #{seq}" if seen[seq]
  seen[seq] = true
  got += val
end
counts = workers.sum(&:value)
raise "FAIL count #{counts}" unless counts == N_JOBS
raise "FAIL sum #{got} != #{expected}" unless got == expected
puts "OK wp_basic_copy"
