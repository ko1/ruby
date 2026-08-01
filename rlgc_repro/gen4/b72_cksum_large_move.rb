# 大きめ payload (16KB x 6) を move で渡し sampling checksum を検証 (payload size 軸)
# axes: 2 workers, move, large strings
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

def scksum(s)
  h = s.bytesize
  i = 0
  while i < s.bytesize
    h = (h * 31 + s.getbyte(i)) & 0xffffffff
    i += 97
  end
  h
end

NB = 6
out = Ractor::Port.new
ws = 2.times.map do
  Ractor.new(out) do |o|
    loop do
      msg = Ractor.receive
      break if msg == :stop
      i, s = msg
      o.send([i, scksum(s)])
    end
  end
end
exp = Array.new(NB)
NB.times do |b|
  s = ("%02d" % b) * (8 * 1024) # 16KB
  exp[b] = scksum(s)
  ws[b % 2].send([b, s], move: true)
end
got = Array.new(NB)
NB.times do
  i, c = out.receive
  got[i] = c
end
ws.each { |w| w.send(:stop) }
ws.each(&:value)
raise "cksums" unless got == exp
puts "OK b72_cksum_large_move"
