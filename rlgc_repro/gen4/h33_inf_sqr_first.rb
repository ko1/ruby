# h33_inf_sqr_first: (1..).lazy square first(k)
# axes: infinite-lazy, endless-range, copy, GC.start
Warning[:experimental] = false
GC.stress = true if ENV['S_STRESS']
def calc_inf_sqr_first
  (1..Float::INFINITY).lazy.map { |x| x * x }.first(8)
end
ref = calc_inf_sqr_first
port = Ractor::Port.new
w = Ractor.new(port) do |po|
  GC.start
  po.send(calc_inf_sqr_first)
  :done
end
got = port.receive
raise "mismatch: #{got.inspect} != #{ref.inspect}" unless got == ref
raise "join" unless w.value == :done
puts "OK h33_inf_sqr_first"
