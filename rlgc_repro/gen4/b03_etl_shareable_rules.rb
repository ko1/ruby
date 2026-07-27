# transform ルール表を make_shareable した lambda 表として参照渡しで全 worker が共有
# axes: 3 workers, shareable reference, copy jobs
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

RULES = Ractor.make_shareable({
  double: Ractor.shareable_lambda { |v| v * 2 },
  square: Ractor.shareable_lambda { |v| v * v },
  negate: Ractor.shareable_lambda { |v| -v },
})
out = Ractor::Port.new
NW = 3
ws = NW.times.map do |wid|
  Ractor.new(out, RULES, wid) do |o, rules, id|
    loop do
      msg = Ractor.receive
      break if msg == :stop
      op, vals = msg
      o.send([id, vals.sum { |v| rules[op].call(v) }])
    end
  end
end
ops = [:double, :square, :negate]
expected = 0
30.times do |k|
  vals = Array.new(8) { |i| k * 10 + i }
  op = ops[k % 3]
  expected += vals.sum { |v| RULES[op].call(v) }
  ws[k % NW].send([op, vals])
end
got = 0
30.times { got += out.receive[1] }
GC.start
ws.each { |w| w.send(:stop) }
ws.each(&:value)
raise "got=#{got} exp=#{expected}" unless got == expected
puts "OK b03_etl_shareable_rules"
