# checksum を運びながら 4 round の value-chain: 各 round の ractor が変換+checksum 更新
# axes: 4 sequential ractors, value join, copy args
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

ROUNDS = 4
data = Array.new(12) { |i| i * 5 + 1 }
h = 0
cur = data
ref = data
refh = 0
ROUNDS.times do |rd|
  r = Ractor.new(cur, h, rd) do |a, hh, k|
    na = a.map { |v| v * 2 + k }
    na.each { |v| hh = (hh * 31 + v) % 1000000007 }
    [na, hh]
  end
  cur, h = r.value
  ref = ref.map { |v| v * 2 + rd }
  ref.each { |v| refh = (refh * 31 + v) % 1000000007 }
end
raise "data" unless cur == ref
raise "cksum #{h} != #{refh}" unless h == refh
puts "OK b74_cksum_value_rounds"
