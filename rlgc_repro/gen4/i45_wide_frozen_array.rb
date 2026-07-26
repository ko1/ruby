# 非常に幅広い frozen array を共有し reader が総和、compact 反復
# axes: elems=5000 readers=3 compacts=4 wide
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
N = 5000
WIDE = Ractor.make_shareable(Array.new(N) { |i| (i * 2 + 1).freeze }.freeze)
EXP = (0...N).sum { |i| i * 2 + 1 }
rs = 3.times.map do |rid|
  Ractor.new(WIDE, rid) do |a, id|
    acc = 0
    3.times { acc += a.sum }
    acc
  end
end
4.times { GC.compact }
rs.each { |ra| raise "mismatch" unless ra.value == EXP * 3 }

puts "OK i45_wide_frozen_array"
