# h12_fiber_pow2: fiber generator: powers of two
# axes: fiber-generator, resume-loop, copy, GC.start
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def mk_fiber_pow2
  Fiber.new do
    v = 1
  loop { Fiber.yield v; v *= 2 }
  end
end
ref = []
f0 = mk_fiber_pow2
21.times { ref << f0.resume }
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  f = mk_fiber_pow2
  out = []
  21.times { |k| out << f.resume; GC.start if k % 6 == 0 }
  po.send(out)
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h12_fiber_pow2"
