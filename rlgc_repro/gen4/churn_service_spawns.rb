# gen4 service+churn: the SERVICE itself spawns a short-lived helper ractor
# for every batch of 20 requests (nested churn inside a long-lived ractor)
# while main-side task churn keeps the request stream coming.
# axes: transfer=copy, GC=GC.start in helpers, exceptions=none, lifecycle=nested churn
N_TASKS = 160

svc = Ractor.new do
  buffer = []
  digests = []
  served = 0
  while (req = Ractor.receive) != :shutdown
    buffer << req
    served += 1
    if buffer.size == 20
      batch = buffer
      buffer = []
      helper = Ractor.new(batch) do |b|
        GC.start
        b.sum { |x| x * 2 }
      end
      digests << helper.value
    end
  end
  raise "leftover #{buffer.size}" unless buffer.empty?
  [served, digests.sum]
end

exp = 0
(N_TASKS / 20).times do |wave|
  tasks = 20.times.map do |t|
    tid = wave * 20 + t
    Ractor.new(svc, tid) do |s, id|
      s << id
      id * 2
    end
  end
  exp += tasks.sum(&:value)
end

svc << :shutdown
served, digest_sum = svc.value
raise "FAIL served" unless served == N_TASKS
raise "FAIL digest #{digest_sum} != #{exp}" unless digest_sum == exp
puts "OK churn_service_spawns"
