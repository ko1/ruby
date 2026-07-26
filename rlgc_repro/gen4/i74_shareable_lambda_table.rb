# make_shareable した lambda 表を全 reader が参照して適用
# axes: readers=5 jobs=30 compacts=5 lambda
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
TABLE = Ractor.make_shareable({
  inc: Ractor.shareable_lambda { |v| v + 1 },
  dbl: Ractor.shareable_lambda { |v| v * 2 },
  sqr: Ractor.shareable_lambda { |v| v * v },
  neg: Ractor.shareable_lambda { |v| -v },
})
OPS = Ractor.make_shareable(%i[inc dbl sqr neg])
def apply_all(table, ops, v)
  ops.sum { |op| table[op].call(v) }
end
JOBS = 30
EXP = (0...JOBS).sum { |v| apply_all(TABLE, OPS, v) }
rs = 5.times.map do |rid|
  Ractor.new(TABLE, OPS, rid) do |table, ops, id|
    (0...JOBS).sum { |v| apply_all(table, ops, v) }
  end
end
5.times { GC.compact }
rs.each { |ra| raise "mismatch" unless ra.value == EXP }

puts "OK i74_shareable_lambda_table"
