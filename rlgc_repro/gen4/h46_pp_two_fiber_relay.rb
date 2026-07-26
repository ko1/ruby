# h46_pp_two_fiber_relay: two fibers relay: B(+100) then A(*2)
# axes: fiber-relay, nested-resume, copy, GC.start
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def run_relay(n)
  b = Fiber.new do |x|
    loop { x = Fiber.yield(x + 100) }
  end
  a = Fiber.new do |x|
    loop { y = b.resume(x); x = Fiber.yield(y * 2) }
  end
  out = []
  cur = 1
  n.times { |k| r = a.resume(cur); out << r; cur = (r % 100) + 1; GC.compact if k % 4 == 0 }
  out
end
ref = run_relay(14)
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  po.send(run_relay(14))
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h46_pp_two_fiber_relay"
