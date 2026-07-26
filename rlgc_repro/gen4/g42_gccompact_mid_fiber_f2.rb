# 2 fibers yield chunks; GC.compact runs between resumes; a Ractor reducer sums delivered bytes
# axes: 1 reducer ractor, 2 fibers, GC.compact, gc between fiber resumes
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
NF = 2
STEPS = 5
port = Ractor::Port.new
reducer = Ractor.new(port) do |po|
  acc = 0
  loop do
    m = Ractor.receive
    break if m == :stop
    acc += m.bytesize
  end
  po.send(acc)
end
fibs = NF.times.map do |i|
  Fiber.new do
    STEPS.times { |j| Fiber.yield(+"fib#{i}-#{j}") }
    nil
  end
end
exp = 0
cnt = 0
alive = [true] * NF
while alive.any?
  NF.times do |i|
    next unless alive[i]
    v2 = fibs[i].resume
    if v2
      exp += v2.bytesize
      reducer.send(v2)
      cnt += 1
      GC.compact if cnt % 4 == 0
    else
      alive[i] = false
    end
  end
end
reducer.send(:stop)
got = port.receive
reducer.value
raise "got=#{got} exp=#{exp}" unless got == exp
puts "OK g42_gccompact_mid_fiber_f2"
