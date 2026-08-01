# h57_fanin_fib_take: fan-in 5 ractors: fanin_fib_take
# axes: multi-ractor, fan-in, lazy/fiber, GC.compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def part_fanin_fib_take(id, n)
  Enumerator.new { |y| a, b = id, 1; loop { y << a; a, b = b, a + b } }.lazy.first(6)
end
port = Ractor::Port.new
ws = (0...5).map do |id|
  Ractor.new(port, id) do |po, id|
    res = part_fanin_fib_take(id, 12)
    GC.compact if id.even?
    po.send([id, res])
    :done
  end
end
got = {}
5.times { id, res = port.receive; got[id] = res }
ws.each(&:value)
ref = (0...5).to_h { |id| [id, part_fanin_fib_take(id, 12)] }
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
puts "OK h57_fanin_fib_take"
