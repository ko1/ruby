# h14_fiber_chars: fiber generator: cycling lowercase chars
# axes: fiber-generator, resume-loop, copy, GC.start
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def mk_fiber_chars
  Fiber.new do
    i = 0
  loop { Fiber.yield((97 + i % 26).chr); i += 1 }
  end
end
ref = []
f0 = mk_fiber_chars
23.times { ref << f0.resume }
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  f = mk_fiber_chars
  out = []
  23.times { |k| out << f.resume; GC.start if k % 6 == 0 }
  po.send(out)
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h14_fiber_chars"
