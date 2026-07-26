# h11_fiber_triangular: fiber generator: triangular numbers
# axes: fiber-generator, resume-loop, copy, GC.start
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def mk_fiber_triangular
  Fiber.new do
    s = 0; i = 1
  loop { s += i; Fiber.yield s; i += 1 }
  end
end
ref = []
f0 = mk_fiber_triangular
20.times { ref << f0.resume }
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  f = mk_fiber_triangular
  out = []
  20.times { |k| out << f.resume; GC.start if k % 6 == 0 }
  po.send(out)
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h11_fiber_triangular"
