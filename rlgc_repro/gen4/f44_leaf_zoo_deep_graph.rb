# f44 fixture generator: depth-5 graph whose leaves span every scalar type, deep equality round-trip
# axes: copy, all leaf kinds, GC.compact
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

LEAVES = [:sym, 42, 2**90, 3.5, Rational(4, 7), Complex(2, 1), (1..9), (5..), "text", nil, true, false].freeze

def zoo(depth, idx)
  return LEAVES[idx % LEAVES.size] if depth == 0
  { l: zoo(depth - 1, idx * 2 + 1), r: [zoo(depth - 1, idx * 2 + 2), idx] }
end

g = zoo(5, 0)
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  mm = Ractor.receive
  GC.compact
  kinds = Hash.new(0)
  walker = lambda do |nn|
    case nn
    when Hash then nn.each_value { |vv| walker.call(vv) }
    when Array then nn.each { |vv| walker.call(vv) }
    else kinds[nn.class.name] += 1
    end
  end
  walker.call(mm)
  po.send([mm, kinds])
end
w.send(g)
back, kinds = port.receive
assert back == g, "deep equality over mixed leaves"
want_kinds = %w[Complex FalseClass Float Integer NilClass Range Rational String Symbol TrueClass]
assert kinds.keys.sort == want_kinds, "leaf kinds present: #{kinds.keys.sort.inspect}"
assert kinds.values.sum == 2**5 + (2**5 - 1), "leaf+idx count"
puts "OK f44_leaf_zoo_deep_graph"
