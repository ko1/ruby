# h10_fiber_squares: fiber generator: perfect squares
# axes: fiber-generator, resume-loop, copy, GC.start
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def mk_fiber_squares
  Fiber.new do
    i = 0
  loop { Fiber.yield(i * i); i += 1 }
  end
end
ref = []
f0 = mk_fiber_squares
19.times { ref << f0.resume }
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  f = mk_fiber_squares
  out = []
  19.times { |k| out << f.resume; GC.start if k % 6 == 0 }
  po.send(out)
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h10_fiber_squares"
