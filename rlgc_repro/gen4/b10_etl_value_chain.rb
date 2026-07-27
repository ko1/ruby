# dataset を Ractor#value の連鎖で段渡しする ETL (stage ごとに ractor を新設)
# axes: 4 stages sequential, value join, copy in / value out
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

data = Array.new(24) { |i| { id: i, v: i * 3 + 1 } }
ref = data.map { |h| h[:v] }
ref = ref.map { |v| v + 7 }.map { |v| v * 2 }.reject { |v| v % 5 == 0 }.map { |v| v - 1 }

stages = [
  Ractor.shareable_lambda { |a| a.map { |h| { id: h[:id], v: h[:v] + 7 } } },
  Ractor.shareable_lambda { |a| a.map { |h| { id: h[:id], v: h[:v] * 2 } } },
  Ractor.shareable_lambda { |a| a.reject { |h| h[:v] % 5 == 0 } },
  Ractor.shareable_lambda { |a| a.map { |h| { id: h[:id], v: h[:v] - 1 } } },
]
cur = data
stages.each_with_index do |st, i|
  r = Ractor.new(cur, st) { |d, f| f.call(d) }
  cur = r.value
  GC.compact if i == 2
end
raise "len" unless cur.size == ref.size
raise "vals" unless cur.map { |h| h[:v] } == ref
puts "OK b10_etl_value_chain"
