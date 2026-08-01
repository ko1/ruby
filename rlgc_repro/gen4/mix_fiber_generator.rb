# gen4 mixed-runtime: pipeline whose source stage generates items lazily from
# an internal Fiber (resume/yield) and whose sink folds with another fiber;
# middle stage is a plain transform ractor.
# axes: transfer=copy, GC=GC.start every 100 generated items, runtime=fibers inside ractors
N_ITEMS = 400

out = Ractor::Port.new

sink = Ractor.new(out) do |o|
  folder = Fiber.new do |first|
    acc = 0
    m = first
    while m != :eos
      acc += m
      m = Fiber.yield
    end
    acc
  end
  result = nil
  loop do
    m = Ractor.receive
    result = folder.resume(m)
    break if m == :eos
  end
  o << result
end

xform = Ractor.new(sink) do |nxt|
  while (m = Ractor.receive) != :eos
    nxt << m[:a] * m[:b]
  end
  nxt << :eos
end

source = Ractor.new(xform, N_ITEMS) do |nxt, n|
  gen = Fiber.new do
    i = 0
    loop do
      Fiber.yield({ a: i, b: (i % 7) + 1 })
      i += 1
      GC.start if i % 100 == 0
    end
  end
  n.times { nxt << gen.resume }
  nxt << :eos
  n
end

exp = (0...N_ITEMS).sum { |i| i * ((i % 7) + 1) }
got = out.receive
raise "FAIL gen count" unless source.value == N_ITEMS
[xform, sink].each(&:join)
raise "FAIL #{got} != #{exp}" unless got == exp
puts "OK mix_fiber_generator"
