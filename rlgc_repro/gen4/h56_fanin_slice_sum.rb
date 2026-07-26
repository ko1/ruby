# h56_fanin_slice_sum: fan-in 4 ractors: fanin_slice_sum
# axes: multi-ractor, fan-in, lazy/fiber, GC.compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def part_fanin_slice_sum(id, n)
  (1..n).each_slice(id + 2).map { |s| s.sum }
end
port = Ractor::Port.new
ws = (0...4).map do |id|
  Ractor.new(port, id) do |po, id|
    res = part_fanin_slice_sum(id, 20)
    GC.compact if id.even?
    po.send([id, res])
    :done
  end
end
got = {}
4.times { id, res = port.receive; got[id] = res }
ws.each(&:value)
ref = (0...4).to_h { |id| [id, part_fanin_slice_sum(id, 20)] }
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
puts "OK h56_fanin_slice_sum"
