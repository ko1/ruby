# gen4 service+churn: tiny variant where the long-lived service runs under
# GC.stress=true while short-lived tasks churn around it.
# axes: transfer=copy, GC=GC.stress in service ractor, lifecycle=churn (small)
N_TASKS = 40

svc = Ractor.new(N_TASKS) do |n|
  GC.stress = true
  sum = 0
  n.times { sum += Ractor.receive }
  GC.stress = false
  sum
end

exp = 0
(N_TASKS / 10).times do |wave|
  tasks = 10.times.map do |t|
    tid = wave * 10 + t
    Ractor.new(svc, tid) do |s, id|
      s << id * 3
      id
    end
  end
  tasks.each(&:join)
  exp += tasks.sum { |x| x.value * 3 }
end

raise "FAIL #{svc.value} != #{exp}" unless svc.value == exp
puts "OK churn_stress_service"
