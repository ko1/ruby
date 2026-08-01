# 継続行 (先頭空白) を直前行に連結してからレコード数と結合後サイズを検証
# axes: 1 assembler + 2 counters, copy, stateful stream parse
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

N = 36
raw = Array.new(N) do |i|
  if i % 5 == 2 || i % 5 == 3
    "  cont-#{i}"
  else
    "head-#{i} start"
  end
end
recs = []
raw.each do |l|
  if l.start_with?(" ") && !recs.empty?
    recs[-1] = recs[-1] + "|" + l.strip
  else
    recs << l.dup
  end
end
exp_cnt = recs.size
exp_bytes = recs.sum(&:bytesize)

sink = Ractor::Port.new
counter = Ractor.new(sink) do |o|
  cnt = 0
  bytes = 0
  while (rec = Ractor.receive) != :eof
    cnt += 1
    bytes += rec.bytesize
  end
  o.send([cnt, bytes])
end
asm = Ractor.new(counter) do |dst|
  pending = nil
  while (l = Ractor.receive) != :eof
    if l.start_with?(" ") && pending
      pending = pending + "|" + l.strip
    else
      dst.send(pending) if pending
      pending = l.dup
    end
  end
  dst.send(pending) if pending
  dst.send(:eof)
end
raw.each { |l| asm.send(l) }
asm.send(:eof)
cnt, bytes = sink.receive
asm.value
counter.value
raise "cnt=#{cnt} exp=#{exp_cnt}" unless cnt == exp_cnt
raise "bytes=#{bytes} exp=#{exp_bytes}" unless bytes == exp_bytes
puts "OK b14_log_multiline_join"
