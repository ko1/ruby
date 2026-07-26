# h39_inf_take_map: (0..).lazy *3 take(k) to_a
# axes: infinite-lazy, endless-range, copy, GC.start
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def calc_inf_take_map
  (0..).lazy.map { |x| x * 3 }.take(9).to_a
end
ref = calc_inf_take_map
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  GC.start
  po.send(calc_inf_take_map)
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h39_inf_take_map"
