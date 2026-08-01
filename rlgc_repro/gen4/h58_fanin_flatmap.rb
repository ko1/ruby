# h58_fanin_flatmap: fan-in 4 ractors: fanin_flatmap
# axes: multi-ractor, fan-in, lazy/fiber, GC.compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def part_fanin_flatmap(id, n)
  (1..n).lazy.flat_map { |x| [x, x * (id + 1)] }.first(6)
end
port = Ractor::Port.new
ws = (0...4).map do |id|
  Ractor.new(port, id) do |po, id|
    res = part_fanin_flatmap(id, 16)
    GC.compact if id.even?
    po.send([id, res])
    :done
  end
end
got = {}
4.times { id, res = port.receive; got[id] = res }
ws.each(&:value)
ref = (0...4).to_h { |id| [id, part_fanin_flatmap(id, 16)] }
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
puts "OK h58_fanin_flatmap"
