# h13_fiber_altsign: fiber generator: alternating signed counter
# axes: fiber-generator, resume-loop, copy, GC.start
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def mk_fiber_altsign
  Fiber.new do
    i = 1
  loop { Fiber.yield(i.even? ? -i : i); i += 1 }
  end
end
ref = []
f0 = mk_fiber_altsign
22.times { ref << f0.resume }
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  f = mk_fiber_altsign
  out = []
  22.times { |k| out << f.resume; GC.start if k % 6 == 0 }
  po.send(out)
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h13_fiber_altsign"
