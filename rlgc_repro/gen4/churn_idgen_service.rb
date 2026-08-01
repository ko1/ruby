# gen4 service+churn: id-generator service answering via per-task reply ports;
# 240 short-lived tasks each request a batch of ids, checks uniqueness, dies.
# axes: transfer=copy, GC=GC.start in service every 100 requests, exceptions=none
N_TASKS = 240
IDS_PER_TASK = 5

idgen = Ractor.new do
  next_id = 0
  served = 0
  while (req = Ractor.receive) != :shutdown
    n, reply = req
    ids = (next_id...(next_id + n)).to_a
    next_id += n
    served += 1
    GC.start if served % 100 == 0
    reply << ids
  end
  [served, next_id]
end

all_ids = []
(N_TASKS / 40).times do |wave|
  tasks = 40.times.map do
    Ractor.new(idgen, IDS_PER_TASK) do |svc, n|
      inbox = Ractor::Port.new
      svc << [n, inbox]
      ids = inbox.receive
      raise "batch size" unless ids.size == n
      ids
    end
  end
  tasks.each { |t| all_ids.concat(t.value) }
end

idgen << :shutdown
served, issued = idgen.value
raise "FAIL served" unless served == N_TASKS
raise "FAIL issued" unless issued == N_TASKS * IDS_PER_TASK
raise "FAIL unique" unless all_ids.uniq.size == all_ids.size
raise "FAIL range" unless all_ids.sort == (0...issued).to_a
puts "OK churn_idgen_service"
