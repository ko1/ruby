# f19 batch runner: send-die-value lifecycle with Struct jobs (stress bounded around #value)
# axes: copy in, result via #value, short-lived ractors
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

Job = Struct.new(:id, :nums)

n = STRESS ? 3 : 8
rs = n.times.map do |i|
  r = Ractor.new do
    jj = Ractor.receive
    Job.new(jj.id, [jj.nums.sum, jj.nums.max])
  end
  r.send(Job.new(i, [i, i + 1, i + 2]))
  r
end

# bound stress: #value under active GC.stress can hit known upstream assert
GC.stress = false if STRESS
rs.each_with_index do |r, i|
  res = r.value
  assert res.is_a?(Job), "value class"
  assert res.id == i, "value id"
  assert res.nums == [3 * i + 3, i + 2], "value nums #{res.nums.inspect}"
end
GC.stress = true if STRESS
GC.start
puts "OK f19_struct_jobs_die_value"
