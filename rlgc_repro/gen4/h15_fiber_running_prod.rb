# h15_fiber_running_prod: fiber generator: running product mod 1000
# axes: fiber-generator, resume-loop, copy, GC.start
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def mk_fiber_running_prod
  Fiber.new do
    p = 1; i = 1
  loop { p = (p * i) % 1000; Fiber.yield p; i += 1 }
  end
end
ref = []
f0 = mk_fiber_running_prod
24.times { ref << f0.resume }
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  f = mk_fiber_running_prod
  out = []
  24.times { |k| out << f.resume; GC.start if k % 6 == 0 }
  po.send(out)
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h15_fiber_running_prod"
