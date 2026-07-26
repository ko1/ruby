# h05_lazy_flatmap_pos: lazy: flat_map +/- then positives, first(k)
# axes: lazy-chain, copy, GC.start, single worker
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
ref = (1..38).flat_map { |x| [x, -x] }.select { |y| y > 0 }.first(5)
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  res = (1..38).lazy.flat_map { |x| [x, -x] }.select { |y| y > 0 }.first(5)
  GC.start
  po.send(res)
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h05_lazy_flatmap_pos"
