# 5 変換を 5 つの ractor value-chain で順に適用し、逐次参照実装と厳密比較
# axes: 5 sequential ractors, shareable_lambda stages, value join
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

STAGES = Ractor.make_shareable([
  Ractor.shareable_lambda { |a| a.map { |v| v + 3 } },
  Ractor.shareable_lambda { |a| a.select(&:even?) },
  Ractor.shareable_lambda { |a| a.map { |v| v * 5 } },
  Ractor.shareable_lambda { |a| a.each_slice(2).map(&:sum) },
  Ractor.shareable_lambda { |a| a.sort.reverse },
])
data = Array.new(20) { |i| (i * 7) % 26 }
ref = data
STAGES.each { |f| ref = f.call(ref) }
cur = data
STAGES.each_with_index do |f, i|
  r = Ractor.new(cur, f) { |a, fn| fn.call(a) }
  cur = r.value
  GC.compact if i == 3
end
raise "chain=#{cur} exp=#{ref}" unless cur == ref
puts "OK b78_multipass_value_chain"
