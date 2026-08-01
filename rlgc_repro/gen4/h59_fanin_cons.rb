# h59_fanin_cons: fan-in 3 ractors: fanin_cons
# axes: multi-ractor, fan-in, lazy/fiber, GC.compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def part_fanin_cons(id, n)
  (1..n).each_cons(id + 2).first(4)
end
port = Ractor::Port.new
ws = (0...3).map do |id|
  Ractor.new(port, id) do |po, id|
    res = part_fanin_cons(id, 18)
    GC.compact if id.even?
    po.send([id, res])
    :done
  end
end
got = {}
3.times { id, res = port.receive; got[id] = res }
ws.each(&:value)
ref = (0...3).to_h { |id| [id, part_fanin_cons(id, 18)] }
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
puts "OK h59_fanin_cons"
