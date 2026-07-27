# 3 段 pipeline の各段が rolling checksum を更新しながら文字列を変換して伝搬
# axes: 3 chained workers, copy, incremental checksum
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

def roll(h, s)
  s.each_byte { |b| h = (h * 131 + b) % 1000000007 }
  h
end

N = 20
final = Ractor::Port.new
s3 = Ractor.new(final) do |o|
  h = 0
  while (s = Ractor.receive) != :eof
    h = roll(h, s)
  end
  o.send(h)
end
s2 = Ractor.new(s3) do |dst|
  h = 0
  while (s = Ractor.receive) != :eof
    h = roll(h, s)
    dst.send(s + "|2")
  end
  dst.send(:eof)
  h
end
s1 = Ractor.new(s2) do |dst|
  h = 0
  while (s = Ractor.receive) != :eof
    h = roll(h, s)
    dst.send(s + "|1")
  end
  dst.send(:eof)
  h
end
msgs = Array.new(N) { |i| "m#{i}" }
exp1 = msgs.reduce(0) { |h, s| roll(h, s) }
exp2 = msgs.reduce(0) { |h, s| roll(h, s + "|1") }
exp3 = msgs.reduce(0) { |h, s| roll(h, s + "|1|2") }
msgs.each { |m| s1.send(m) }
s1.send(:eof)
h3 = final.receive
h1 = s1.value
h2 = s2.value
s3.value
raise "h1" unless h1 == exp1
raise "h2" unless h2 == exp2
raise "h3" unless h3 == exp3
puts "OK b71_cksum_rolling_chain"
