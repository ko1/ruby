# 行ごとの adler 風 checksum を worker で計算し、順序どおり fold して一致検証
# axes: 4 workers, copy, indexed reorder + fold (checksum は算術のみで alloc なし)
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

def adler(s)
  a = 1
  b = 0
  s.each_byte do |c|
    a = (a + c) % 65521
    b = (b + a) % 65521
  end
  (b << 16) | a
end

N = 30
NW = 4
lines = Array.new(N) { |i| "line-#{i}:" + ("q" * (i % 29)) }
exp = lines.map { |l| adler(l) }.reduce(0) { |acc, c| (acc * 31 + c) & 0xffffffff }

out = Ractor::Port.new
ws = NW.times.map do
  Ractor.new(out) do |o|
    loop do
      msg = Ractor.receive
      break if msg == :stop
      i, l = msg
      o.send([i, adler(l)])
    end
  end
end
lines.each_with_index { |l, i| ws[i % NW].send([i, l]) }
sums = Array.new(N)
N.times do
  i, c = out.receive
  sums[i] = c
end
ws.each { |w| w.send(:stop) }
ws.each(&:value)
got = sums.reduce(0) { |acc, c| (acc * 31 + c) & 0xffffffff }
raise "cksum #{got} != #{exp}" unless got == exp
puts "OK b20_log_line_checksums"
