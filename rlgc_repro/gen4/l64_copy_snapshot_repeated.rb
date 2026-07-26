# large copy snapshot under compaction; sender retains object
# axes: copy, snapshot, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
port = Ractor::Port.new
w = Ractor.new(port) do |o|
  total = 0
  8.times { total += Ractor.receive.sum }
  o.send(total)
end
exp = 0
8.times do |k|
  a = Array.new(150) { |i| k * 150 + i }
  exp += a.sum
  w.send(a)  # copy each
  GC.compact
end
res = port.receive; w.value
raise "rep #{res}!=#{exp}" unless res == exp
puts "OK l64_copy_snapshot_repeated"
