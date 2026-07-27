# gen4 server-sim: handlers run GC.start/GC.compact on a request-count cadence
# while holding live per-key state; main also compacts twice mid-traffic.
# axes: transfer=copy, GC=GC.start+GC.compact in handlers and main, exceptions=none
N_HANDLERS = 4
N_REQS = 360

handlers = N_HANDLERS.times.map do |hid|
  Ractor.new(hid) do |id|
    state = Hash.new { |h, k| h[k] = [] }
    n = 0
    while (req = Ractor.receive) != :shutdown
      state[req[:key]] << req[:data]
      state[req[:key]].shift if state[req[:key]].size > 6
      n += 1
      GC.start if n % 45 == 0
      GC.compact if id.even? && n % 70 == 0
      req[:reply] << req[:data].sum
    end
    [n, state.values.sum(&:size)]
  end
end

reply = Ractor::Port.new
expected = 0
N_REQS.times do |i|
  data = [i, i % 9, 7]
  expected += data.sum
  handlers[i % N_HANDLERS] << { key: "k#{i % 15}", data: data, reply: reply }
  GC.compact if i == 120 || i == 300
end

got = 0
N_REQS.times { got += reply.receive }
handlers.each { |h| h << :shutdown }
stats = handlers.map(&:value)
raise "FAIL handled" unless stats.sum(&:first) == N_REQS
raise "FAIL sum #{got} != #{expected}" unless got == expected
puts "OK srv_gc_handlers"
