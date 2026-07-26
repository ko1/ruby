# h40_inf_filter_map: (1..).lazy filter_map even*2 first(k)
# axes: infinite-lazy, endless-range, copy, GC.start
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def calc_inf_filter_map
  (1..).lazy.filter_map { |x| x * 2 if x.even? }.first(7)
end
ref = calc_inf_filter_map
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  GC.start
  po.send(calc_inf_filter_map)
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h40_inf_filter_map"
