# h44_pp_accumulate: fiber resume feedback: running add
# axes: fiber-pingpong, resume-arg, copy, GC.start
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def run_pp_accumulate(n)
  f = Fiber.new do |x|
    acc = 0
    loop { acc += x; x = Fiber.yield(acc) }
  end
  acc = []
  cur = 1
  n.times { |k| r = f.resume(cur); acc << r; cur = r % 50; GC.start if k % 5 == 0 }
  acc
end
ref = run_pp_accumulate(15)
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  po.send(run_pp_accumulate(15))
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h44_pp_accumulate"
