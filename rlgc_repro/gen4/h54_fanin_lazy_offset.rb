# h54_fanin_lazy_offset: fan-in 5 ractors: fanin_lazy_offset
# axes: multi-ractor, fan-in, lazy/fiber, GC.compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def part_fanin_lazy_offset(id, n)
  (1..n).lazy.map { |x| x + id * 10 }.select { |y| y % 3 == 0 }.first(4)
end
port = Ractor::Port.new
ws = (0...5).map do |id|
  Ractor.new(port, id) do |po, id|
    res = part_fanin_lazy_offset(id, 20)
    GC.compact if id.even?
    po.send([id, res])
    :done
  end
end
got = {}
5.times { id, res = port.receive; got[id] = res }
ws.each(&:value)
ref = (0...5).to_h { |id| [id, part_fanin_lazy_offset(id, 20)] }
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
puts "OK h54_fanin_lazy_offset"
