# h09_fiber_fibonacci: fiber generator: fibonacci sequence
# axes: fiber-generator, resume-loop, copy, GC.start
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def mk_fiber_fibonacci
  Fiber.new do
    a, b = 0, 1
  loop { Fiber.yield a; a, b = b, a + b }
  end
end
ref = []
f0 = mk_fiber_fibonacci
18.times { ref << f0.resume }
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  f = mk_fiber_fibonacci
  out = []
  18.times { |k| out << f.resume; GC.start if k % 6 == 0 }
  po.send(out)
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h09_fiber_fibonacci"
