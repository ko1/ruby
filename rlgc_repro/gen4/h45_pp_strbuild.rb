# h45_pp_strbuild: fiber resume feedback: string length
# axes: fiber-pingpong, resume-arg, copy, GC.start
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def run_pp_strbuild(n)
  f = Fiber.new do |x|
    s = +""
    loop { s << x.to_s; x = Fiber.yield(s.size) }
  end
  acc = []
  cur = 1
  n.times { |k| r = f.resume(cur); acc << r; cur = (r + 3) % 40; GC.start if k % 5 == 0 }
  acc
end
ref = run_pp_strbuild(16)
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  po.send(run_pp_strbuild(16))
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h45_pp_strbuild"
