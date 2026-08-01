# h41_pp_double1: fiber resume feedback: x*2+1
# axes: fiber-pingpong, resume-arg, copy, GC.start
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def run_pp_double1(n)
  f = Fiber.new do |x|
    loop { x = Fiber.yield(x * 2 + 1) }
  end
  acc = []
  cur = 1
  n.times { |k| r = f.resume(cur); acc << r; cur = r % 100; GC.start if k % 5 == 0 }
  acc
end
ref = run_pp_double1(12)
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  po.send(run_pp_double1(12))
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h41_pp_double1"
