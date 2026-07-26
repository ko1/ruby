# h62_fanin_takewhile: fan-in 6 ractors: fanin_takewhile
# axes: multi-ractor, fan-in, lazy/fiber, GC.compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def part_fanin_takewhile(id, n)
  (1..n).lazy.map { |x| x * 2 }.take_while { |y| y < (id + 1) * 8 }.to_a
end
port = Ractor::Port.new
ws = (0...6).map do |id|
  Ractor.new(port, id) do |po, id|
    res = part_fanin_takewhile(id, 40)
    GC.compact if id.even?
    po.send([id, res])
    :done
  end
end
got = {}
6.times { id, res = port.receive; got[id] = res }
ws.each(&:value)
ref = (0...6).to_h { |id| [id, part_fanin_takewhile(id, 40)] }
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
puts "OK h62_fanin_takewhile"
