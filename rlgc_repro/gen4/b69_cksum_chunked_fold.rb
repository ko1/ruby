# chunk ごとの checksum を並列計算し、順序 fold で全体 checksum を再構成して比較
# axes: 3 workers, copy, indexed fold
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

def cksum(s)
  h = 5381
  s.each_byte { |b| h = ((h * 33) ^ b) & 0xffffffff }
  h
end

NC = 12
chunks = Array.new(NC) { |c| "chunk#{c}-" + ("d" * (c % 11 + 4)) }
exp = chunks.map { |c| cksum(c) }.reduce(0) { |a, c| (a * 65599 + c) & 0xffffffff }

out = Ractor::Port.new
ws = 3.times.map do
  Ractor.new(out) do |o|
    loop do
      msg = Ractor.receive
      break if msg == :stop
      i, s = msg
      o.send([i, cksum(s)])
    end
  end
end
chunks.each_with_index { |c, i| ws[i % 3].send([i, c]) }
parts = Array.new(NC)
NC.times do
  i, c = out.receive
  parts[i] = c
end
ws.each { |w| w.send(:stop) }
ws.each(&:value)
got = parts.reduce(0) { |a, c| (a * 65599 + c) & 0xffffffff }
raise "cksum #{got} != #{exp}" unless got == exp
puts "OK b69_cksum_chunked_fold"
