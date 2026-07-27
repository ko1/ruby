# CRC 風 256 要素テーブルを make_shareable で共有し table-driven checksum を並列計算
# axes: 2 workers, shareable lookup table, copy
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']

TBL = Ractor.make_shareable(Array.new(256) { |i| (i * 2654435761) & 0xffffffff })
def tcksum(s, tbl)
  h = 0xffffffff
  s.each_byte { |b| h = (tbl[(h ^ b) & 0xff] ^ (h >> 8)) & 0xffffffff }
  h
end

N = 20
strs = Array.new(N) { |i| "data-#{i}-" + ("e" * (i % 13)) }
exp = strs.sum { |s| tcksum(s, TBL) }

out = Ractor::Port.new
ws = 2.times.map do |wi|
  Ractor.new(out, TBL, strs.each_slice(2).select.with_index { |_, k| k % 2 == wi }.flatten) do |o, tbl, part|
    o.send(part.sum { |s| tcksum(s, tbl) })
  end
end
got = 0
2.times { got += out.receive }
ws.each(&:value)
raise "cksum #{got} != #{exp}" unless got == exp
puts "OK b70_cksum_crc_table"
