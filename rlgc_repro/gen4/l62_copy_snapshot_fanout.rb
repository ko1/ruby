# large copy snapshot under compaction; sender retains object
# axes: copy, snapshot, compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
base = Array.new(400) { |i| i * 2 }
ports = []; ws = []
4.times do
  p = Ractor::Port.new; ports << p
  ws << Ractor.new(p) { |o| a = Ractor.receive; o.send(a.sum) }
end
ws.each { |w| w.send(base) }
GC.compact
exp = base.sum
ports.each { |p| raise unless p.receive == exp }
ws.each(&:value)
raise unless base.sum == exp
puts "OK l62_copy_snapshot_fanout"
