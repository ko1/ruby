# h49_nest_mul2_d7: nested fibers depth 7
# axes: fiber-depth, recursive-resume, copy, GC.start
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def build_nest_mul2_d7(d)
  Fiber.new do
    if d == 0
      Fiber.yield(1)
    else
      child = build_nest_mul2_d7(d - 1)
      Fiber.yield(child.resume * 2)
    end
  end
end
ref = build_nest_mul2_d7(7).resume
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  GC.start
  v = build_nest_mul2_d7(7).resume
  po.send(v)
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h49_nest_mul2_d7"
