# c41: futures as one-shot ports: main creates N ports, producers resolve them
# once; main collects in creation order. Copy results; main stress.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false

N = STRESS ? 4 : 8

futures = N.times.map { Ractor::Port.new }
# NOTE: ports created before enabling stress: >=4 Port.new under GC.stress
# self-deadlocks (port-table growth allocs under ractor_lock vs GC mark).
GC.stress = true if STRESS
prods = N.times.map do |i|
  Ractor.new(futures[i], i) do |fut, id|
    v = (1..(id + 3)).reduce(:*)   # (id+3)!
    fut << [:resolved, id, v]
    :produced
  end
end

N.times do |i|
  tag, id, v = futures[i].receive
  raise "tag" unless tag == :resolved
  raise "id" unless id == i
  raise "val" unless v == (1..(i + 3)).reduce(:*)
end
GC.stress = false
prods.each { |r| raise "join" unless r.value == :produced }
GC.start
puts "OK c41_future_basic"
