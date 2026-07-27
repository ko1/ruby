# c45: future resolved with a moved mutable result (large string + array); main
# proves ownership by mutating the received objects.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

N = STRESS ? 3 : 5
LEN = STRESS ? 256 : 4096

futures = N.times.map { Ractor::Port.new }
prods = N.times.map do |i|
  Ractor.new(futures[i], i, LEN) do |fut, id, len|
    s = "x#{id}" * len
    arr = Array.new(len / 8) { |k| k + id }
    fut.send([:resolved, s, arr], move: true)
    :produced
  end
end

N.times do |i|
  tag, s, arr = futures[i].receive
  raise "tag" unless tag == :resolved
  raise "len" unless s.size == LEN * 2
  raise "content" unless s.start_with?("x#{i}x#{i}")
  s << "-mutated"
  arr << -1
  raise "mut" unless s.end_with?("-mutated") && arr.last == -1
  raise "arr" unless arr[3] == 3 + i
end
GC.stress = false
prods.each { |r| raise unless r.value == :produced }
puts "OK c45_future_move_result"
