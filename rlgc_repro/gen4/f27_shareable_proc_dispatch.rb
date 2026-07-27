# f27 rule engine: shareable binding-free lambdas in a frozen dispatch table, called in workers
# axes: shareable Proc via make_shareable, dispatch by symbol, GC.start
Warning[:experimental] = false
STRESS = ENV['S_STRESS']
GC.stress = true if STRESS

def assert(cond, msg = "assert")
  raise msg unless cond
end

module Rules
  def self.table
    { double: ->(x) { x * 2 },
      square: ->(x) { x * x },
      negate: ->(x) { -x },
      clamp:  ->(x) { x > 100 ? 100 : x } }
  end
end

RULES = Ractor.make_shareable(Rules.table)
assert Ractor.shareable?(RULES[:double]), "lambda shareable"

port = Ractor::Port.new
w = Ractor.new(port, RULES) do |po, tbl|
  loop do
    mm = Ractor.receive
    break if mm == :eof
    op, arg = mm
    po.send([op, tbl[op].call(arg)])
  end
end

jobs = [[:double, 21], [:square, 9], [:negate, 5], [:clamp, 500]]
jobs.each { |jj| w.send(jj) }
GC.start
want = { double: 42, square: 81, negate: -5, clamp: 100 }
4.times do
  op, res = port.receive
  assert res == want[op], "#{op} => #{res}"
end
# main can call them too
assert RULES[:square].call(12) == 144, "local call"
w.send(:eof)
puts "OK f27_shareable_proc_dispatch"
