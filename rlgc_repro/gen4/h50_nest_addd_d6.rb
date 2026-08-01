# h50_nest_addd_d6: nested fibers depth 6
# axes: fiber-depth, recursive-resume, copy, GC.start
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def build_nest_addd_d6(d)
  Fiber.new do
    if d == 0
      Fiber.yield(0)
    else
      child = build_nest_addd_d6(d - 1)
      Fiber.yield(child.resume + d)
    end
  end
end
ref = build_nest_addd_d6(6).resume
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  GC.start
  v = build_nest_addd_d6(6).resume
  po.send(v)
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h50_nest_addd_d6"
