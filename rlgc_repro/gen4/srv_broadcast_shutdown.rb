# gen4 server-sim: pub/sub-ish broadcast. Publishers send to a hub; the hub
# fans every message out to all subscribers; graceful multi-phase shutdown
# (publishers drain -> hub broadcasts :bye -> subscribers report via #value).
# axes: transfer=copy, GC=one GC.start in hub mid-run, exceptions=none, payload=small hashes
N_SUBS = 5
N_PUBS = 3
MSGS_PER_PUB = 80

subs = N_SUBS.times.map do |sid|
  Ractor.new(sid) do |_id|
    n = sum = 0
    while (m = Ractor.receive) != :bye
      n += 1
      sum += m[:v]
    end
    [n, sum]
  end
end

hub = Ractor.new(subs, N_PUBS) do |ss, npubs|
  fin = 0
  fanned = 0
  loop do
    m = Ractor.receive
    if m == :pub_done
      fin += 1
      break if fin == npubs
      next
    end
    ss.each { |s| s << m }
    fanned += 1
    GC.start if fanned == 100
  end
  ss.each { |s| s << :bye }
  fanned
end

pubs = N_PUBS.times.map do |pid|
  Ractor.new(hub, pid, MSGS_PER_PUB) do |h, id, n|
    n.times { |i| h << { from: id, v: id * 10_000 + i } }
    h << :pub_done
    n
  end
end

sent = pubs.sum(&:value)
fanned = hub.value
per_sub = subs.map(&:value)
exp_sum = N_PUBS.times.sum { |p| MSGS_PER_PUB.times.sum { |i| p * 10_000 + i } }
raise "FAIL sent" unless sent == N_PUBS * MSGS_PER_PUB
raise "FAIL fanned" unless fanned == sent
per_sub.each do |n, sum|
  raise "FAIL sub n #{n}" unless n == sent
  raise "FAIL sub sum" unless sum == exp_sum
end
puts "OK srv_broadcast_shutdown"
