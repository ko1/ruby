# gen4 mixed-runtime: inside each worker ractor, a PRODUCER THREAD receives
# messages from the ractor's port while the main thread consumes from an
# internal queue and ships results out (thread<->port interplay).
# axes: transfer=copy, GC=GC.start in consumer thread, runtime=threads + port receive off-main-thread
N_WORKERS = 3
JOBS = 150

results = Ractor::Port.new
workers = N_WORKERS.times.map do |wid|
  Ractor.new(results, wid, JOBS) do |res, id, njobs|
    inbox = Ractor::Port.new
    Ractor.main << [:register, id, inbox] # hand our private port to main
    q = Thread::Queue.new
    producer = Thread.new do
      loop do
        m = inbox.receive # port receive on a non-main thread
        q << m
        break if m == :stop
      end
    end
    done = 0
    sum = 0
    while (m = q.pop) != :stop
      sum += m[:x] * m[:y]
      done += 1
      GC.start if done % 60 == 0
    end
    producer.join
    res << [id, done, sum]
    :bye
  end
end

inboxes = {}
N_WORKERS.times do
  tag, id, port = Ractor.receive
  raise "register" unless tag == :register
  inboxes[id] = port
end

exp = Array.new(N_WORKERS, 0)
JOBS.times do |i|
  N_WORKERS.times do |w|
    exp[w] += i * (w + 2)
    inboxes[w] << { x: i, y: w + 2 }
  end
end
inboxes.each_value { |p| p << :stop }

N_WORKERS.times do
  id, done, sum = results.receive
  raise "FAIL done w#{id}" unless done == JOBS
  raise "FAIL sum w#{id}: #{sum} != #{exp[id]}" unless sum == exp[id]
end
workers.each { |w| raise "FAIL exit" unless w.value == :bye }
puts "OK mix_thread_producer"
