# h16_fiber_collatz_steps: fiber generator: collatz step counts
# axes: fiber-generator, resume-loop, copy, GC.start
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def mk_fiber_collatz_steps
  Fiber.new do
    i = 1
  loop { c = i; n = 0; while c != 1; c = c.even? ? c/2 : 3*c+1; n += 1; end; Fiber.yield n; i += 1 }
  end
end
ref = []
f0 = mk_fiber_collatz_steps
25.times { ref << f0.resume }
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  f = mk_fiber_collatz_steps
  out = []
  25.times { |k| out << f.resume; GC.start if k % 6 == 0 }
  po.send(out)
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h16_fiber_collatz_steps"
