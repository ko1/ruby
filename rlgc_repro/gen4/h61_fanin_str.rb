# h61_fanin_str: fan-in 5 ractors: fanin_str
# axes: multi-ractor, fan-in, lazy/fiber, GC.compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def part_fanin_str(id, n)
  (1..n).lazy.map { |x| "r#{id}-#{x}" }.first(5)
end
port = Ractor::Port.new
ws = (0...5).map do |id|
  Ractor.new(port, id) do |po, id|
    res = part_fanin_str(id, 15)
    GC.compact if id.even?
    po.send([id, res])
    :done
  end
end
got = {}
5.times { id, res = port.receive; got[id] = res }
ws.each(&:value)
ref = (0...5).to_h { |id| [id, part_fanin_str(id, 15)] }
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
puts "OK h61_fanin_str"
