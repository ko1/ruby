# h37_inf_flatmap_dup: (1..).lazy flat_map dup first(k)
# axes: infinite-lazy, endless-range, copy, GC.start
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def calc_inf_flatmap_dup
  (1..).lazy.flat_map { |x| [x, x] }.first(9)
end
ref = calc_inf_flatmap_dup
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  GC.start
  po.send(calc_inf_flatmap_dup)
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h37_inf_flatmap_dup"
