# c42: promise pipelining: A's result feeds B directly, B's feeds C, C resolves
# main's future port; per-stage transforms asserted end-to-end.
Warning[:experimental] = false
STRESS = ENV['S_STRESS'] ? true : false
GC.stress = true if STRESS

ROUNDS = STRESS ? 3 : 10
fut = Ractor::Port.new

c = Ractor.new(fut, ROUNDS) do |f, n|
  n.times do
    tag, x = Ractor.receive
    raise "c tag" unless tag == :from_b
    f << [:final, x + 7]
  end
  :c_done
end
b = Ractor.new(c, ROUNDS) do |nxt, n|
  n.times do
    tag, x = Ractor.receive
    raise "b tag" unless tag == :from_a
    nxt.send([:from_b, x * 3])
  end
  :b_done
end
a = Ractor.new(b, ROUNDS) do |nxt, n|
  n.times do
    tag, x = Ractor.receive
    raise "a tag" unless tag == :seed
    nxt.send([:from_a, x + 1])
  end
  :a_done
end

ROUNDS.times do |i|
  a.send([:seed, i])
  tag, v = fut.receive
  raise "final tag" unless tag == :final
  raise "final #{v}" unless v == (i + 1) * 3 + 7
end
GC.stress = false
raise unless a.value == :a_done && b.value == :b_done && c.value == :c_done
puts "OK c42_future_pipeline"
