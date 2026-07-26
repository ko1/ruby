# build nested structures, Ractor.make_shareable under GC.compact pressure, then 5 readers sum shared bytes
# axes: 5 readers, shareable share, GC.compact, gc during make_shareable
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
NR = 5
M = 16
shared = M.times.map do |k|
  GC.compact if k % 4 == 0
  Ractor.make_shareable({ id: k, parts: Array.new(6) { |i| +"s#{k}-#{i}" } })
end
exp = shared.sum { |h| h[:parts].sum(&:bytesize) }
readers = NR.times.map do |rid|
  Ractor.new(shared, rid, NR) do |data, id, n|
    s = 0
    data.each_with_index { |h, k| s += h[:parts].sum(&:bytesize) if k % n == id }
    s
  end
end
got = readers.sum(&:value)
raise "got=#{got} exp=#{exp}" unless got == exp
puts "OK g35_gccompact_during_makeshareable_r5"
