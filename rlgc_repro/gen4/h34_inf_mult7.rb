# h34_inf_mult7: (1..).lazy multiples of 7 first(k)
# axes: infinite-lazy, endless-range, copy, GC.start
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def calc_inf_mult7
  (1..).lazy.select { |x| x % 7 == 0 }.first(8)
end
ref = calc_inf_mult7
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  GC.start
  po.send(calc_inf_mult7)
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h34_inf_mult7"
