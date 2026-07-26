# h51_nest_mul3_d4: nested fibers depth 4
# axes: fiber-depth, recursive-resume, copy, GC.start
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def build_nest_mul3_d4(d)
  Fiber.new do
    if d == 0
      Fiber.yield(1)
    else
      child = build_nest_mul3_d4(d - 1)
      Fiber.yield(child.resume * 3)
    end
  end
end
ref = build_nest_mul3_d4(4).resume
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  GC.start
  v = build_nest_mul3_d4(4).resume
  po.send(v)
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h51_nest_mul3_d4"
