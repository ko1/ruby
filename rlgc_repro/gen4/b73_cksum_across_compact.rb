# worker が保持するデータの checksum を GC.compact 前後で取り不変を検証
# axes: 2 workers, copy, compact 耐性 (checksum before/after)
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

def dcksum(arr)
  h = 7
  arr.each { |s| s.each_byte { |b| h = (h * 131 + b) % 1000000007 } }
  h
end

out = Ractor::Port.new
ws = 2.times.map do |wi|
  Ractor.new(out, wi) do |o, id|
    data = Array.new(15) { |i| "hold-#{id}-#{i}-" + ("f" * (i % 7 + 2)) }
    before = dcksum(data)
    GC.compact
    GC.start
    after = dcksum(data)
    o.send([id, before, after])
  end
end
GC.compact
2.times do
  id, before, after = out.receive
  raise "r#{id}: cksum changed #{before} -> #{after}" unless before == after
  # 内容も独立に再計算して一致確認
  ref = Array.new(15) { |i| "hold-#{id}-#{i}-" + ("f" * (i % 7 + 2)) }
  raise "r#{id}: cksum wrong" unless before == dcksum(ref)
end
ws.each(&:value)
puts "OK b73_cksum_across_compact"
