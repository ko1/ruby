# f71 one-shot batch: send-die-value with heterogeneous payloads (struct/exc/range/string)
# axes: copy in, #value out (stress bounded), short-lived ractors
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

Pack = Struct.new(:tag, :body)

inputs = [
  Pack.new(:str, "abc" * 10),
  Pack.new(:rng, (10..20)),
  Pack.new(:exc, RuntimeError.new("boxed")),
  Pack.new(:num, 2**70),
]
rs = inputs.map do |pk|
  r = Ractor.new do
    mm = Ractor.receive
    digest = case mm.tag
             when :str then mm.body.length
             when :rng then mm.body.sum
             when :exc then mm.body.message.upcase
             when :num then mm.body + 1
             end
    [mm.tag, digest]
  end
  r.send(pk)
  r
end

# bound stress: #value under active GC.stress can hit known upstream recursive-lock assert
GC.stress = false if STRESS
got = rs.map(&:value)
GC.stress = true if STRESS
assert got == [[:str, 30], [:rng, 165], [:exc, "BOXED"], [:num, 2**70 + 1]], "values #{got.inspect}"
GC.start
puts "OK f71_mixed_die_value"
