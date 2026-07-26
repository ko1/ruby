# h55_fanin_lazy_mod: fan-in 6 ractors: fanin_lazy_mod
# axes: multi-ractor, fan-in, lazy/fiber, GC.compact
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def part_fanin_lazy_mod(id, n)
  (1..n).lazy.select { |x| x % (id + 2) == 0 }.first(5)
end
port = Ractor::Port.new
ws = (0...6).map do |id|
  Ractor.new(port, id) do |po, id|
    res = part_fanin_lazy_mod(id, 30)
    GC.compact if id.even?
    po.send([id, res])
    :done
  end
end
got = {}
6.times { id, res = port.receive; got[id] = res }
ws.each(&:value)
ref = (0...6).to_h { |id| [id, part_fanin_lazy_mod(id, 30)] }
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
puts "OK h55_fanin_lazy_mod"
